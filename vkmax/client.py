import asyncio
import itertools
import json
import logging
import uuid
from typing import Any, Optional

import aiohttp
import websockets
from websockets import ConnectionClosedOK, ConnectionClosedError, Close
from websockets.asyncio.client import ClientConnection
from functools import wraps

WS_HOST = "wss://ws-api.oneme.ru/websocket"
RPC_VERSION = 11
APP_VERSION = "25.11.2"
USER_AGENT = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Safari/537.36"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

_logger = logging.getLogger("MaxClient")


def ensure_connected(method):
    @wraps(method)
    async def wrapper(self, *args, **kwargs):
        if self._closing:
            raise RuntimeError("Client is closing")
        if not self._connected:
            if self._reconnect_task:
                _logger.info("Waiting for reconnect before sending...")
                try:
                    await asyncio.wait_for(self._reconnect_task, timeout=30)
                except asyncio.TimeoutError:
                    raise ConnectionClosedError(
                        rcvd=Close(code=1006, reason="reconnect timeout"),
                        sent=None
                    )
            else:
                raise ConnectionClosedError(rcvd=Close(code=1006, reason="connection lost"), sent=None)
        try:
            return await method(self, *args, **kwargs)
        except ConnectionClosedError:
            if not self._closing:
                if not self._reconnect_task or self._reconnect_task.done():
                    self._reconnect_task = asyncio.create_task(self._reconnect(), name="ws-reconnect")
                _logger.info("Connection lost during send, waiting reconnect...")
                await self._reconnect_task
                return await method(self, *args, **kwargs)
            else:
                raise

    return wrapper


class MaxClient:
    def __init__(self, device_id: str = str(uuid.uuid4()), app_version: str = APP_VERSION):
        self.device_id = device_id
        self.app_version = app_version

        self._connection: Optional[ClientConnection] = None
        self._http_pool: Optional[aiohttp.ClientSession] = None
        self._keepalive_task: Optional[asyncio.Task] = None
        self._recv_task: Optional[asyncio.Task] = None
        self._reconnect_task: Optional[asyncio.Task] = None
        self._incoming_event_callback = None
        self._pending: dict[int, asyncio.Future] = {}
        self._logout_future: Optional[asyncio.Future] = None

        self._seq = itertools.count(1)
        self._session_token: Optional[str] = None
        self._is_logged_in: bool = False

        self._video_pending = {}
        self._file_pending = {}
        self._cached_chats = None
        self._cached_contacts = None
        self._cached_favourite_chats = None

        self._connect_lock = asyncio.Lock()
        self._reconnect_lock = asyncio.Lock()

        self._closing = False
        self._connected = False

    def _fail_all_pending(self, exc: Exception):
        for fut in self._pending.values():
            if not fut.done():
                fut.set_exception(exc)
        self._pending.clear()

    async def set_app_version(self, appversion):
        self.app_version = appversion

    async def set_callback(self, function):
        if not asyncio.iscoroutinefunction(function):
            raise TypeError('callback must be async')
        self._incoming_event_callback = function

    # ---------------- WebSocket ----------------
    async def connect(self):
        async with self._connect_lock:
            if self._connection:
                return

            _logger.info(f"Connecting to {WS_HOST}...")
            self._connection = await websockets.connect(
                WS_HOST,
                origin=websockets.Origin('https://web.max.ru'),
                user_agent_header=USER_AGENT,
                ping_interval=None
            )
            self._connected = True
            self._closing = False

            self._recv_task = asyncio.create_task(self._recv_loop(), name="ws-recv-loop")
            self._keepalive_task = asyncio.create_task(self._keepalive_loop(), name="ws-keepalive")
            await self._send_hello_packet()
            _logger.info("WebSocket connected")

    async def _reconnect(self):
        async with self._reconnect_lock:
            if self._closing:
                return
            _logger.warning("Starting reconnect sequence")
            backoff = 2
            while not self._closing:
                try:
                    await self.disconnect()
                except Exception:
                    pass
                await asyncio.sleep(backoff)
                try:
                    await self.connect()
                    if self._session_token:
                        await self._login_by_token_internal(self._session_token)
                    _logger.warning("Reconnect successful")
                    return
                except Exception as e:
                    _logger.error(f"Reconnect failed: {e}")
                    backoff = min(backoff * 2, 30)

    async def disconnect(self):
        if self._connection is None:
            return
        try:
            self._closing = True
            self._connected = False
            if self._keepalive_task:
                self._keepalive_task.cancel()
            if self._recv_task:
                self._recv_task.cancel()
            try:
                await self._connection.close()
            except Exception:
                pass
            if self._http_pool:
                await self._http_pool.close()
                self._http_pool = None
        except Exception:
            pass
        finally:
            self._connection = None
            self._keepalive_task = None
            self._recv_task = None

    @ensure_connected
    async def invoke_method(self, opcode: int, payload: dict[str, Any]):
        seq = next(self._seq)
        fut = asyncio.get_running_loop().create_future()
        self._pending[seq] = fut
        try:
            await self._connection.send(json.dumps({
                "ver": RPC_VERSION,
                "cmd": 0,
                "seq": seq,
                "opcode": opcode,
                "payload": payload
            }))
        except ConnectionClosedError:
            raise
        return await fut

    async def _recv_loop(self):
        try:
            async for raw in self._connection:
                try:
                    packet = json.loads(raw)
                except Exception:
                    continue

                seq = packet.get("seq")
                if seq is not None:
                    fut = self._pending.pop(seq, None)
                    if fut and not fut.done():
                        fut.set_result(packet)
                    continue

                opcode = packet.get("opcode")
                payload = packet.get("payload", {})

                if opcode == 20 and self._logout_future and not self._logout_future.done():
                    self._logout_future.set_result(True)

                if opcode == 136:
                    future = None
                    if "videoId" in payload:
                        future = self._video_pending.pop(payload["videoId"], None)
                    elif "fileId" in payload:
                        future = self._file_pending.pop(payload["fileId"], None)
                    if future and not future.done():
                        future.set_result(None)

                if self._incoming_event_callback:
                    try:
                        asyncio.create_task(self._incoming_event_callback(self, packet))
                    except Exception as e:
                        _logger.exception(f"Failed to schedule incoming_event_callback: {e}")
        except asyncio.CancelledError:
            return
        except (ConnectionClosedError, ConnectionClosedOK) as e:
            _logger.warning(f"WebSocket closed: {e}")
        except Exception as e:
            _logger.exception(f"Recv loop crashed: {e}")
        finally:
            self._connected = False
            if not self._closing:
                self._connection = None
                self._fail_all_pending(
                    ConnectionClosedError(rcvd=Close(code=1006, reason="connection lost"), sent=None))
                if not self._reconnect_task or self._reconnect_task.done():
                    self._reconnect_task = asyncio.create_task(
                        self._reconnect(), name="ws-reconnect"
                    )
            else:
                _logger.info("Connection closed intentionally, no reconnect")

    # --- Keepalive system
    async def _keepalive_loop(self):
        try:
            while True:
                if self._connection is None:
                    return
                await self._connection.send(json.dumps({
                    "ver": RPC_VERSION,
                    "cmd": 0,
                    "seq": next(self._seq),
                    "opcode": 1,
                    "payload": {"interactive": False}
                }))
                await asyncio.sleep(12)
        except asyncio.CancelledError:
            return
        except Exception:
            return

    async def _start_keepalive_task(self):
        if not self._keepalive_task:
            self._keepalive_task = asyncio.create_task(self._keepalive_loop())

    async def _stop_keepalive_task(self):
        if self._keepalive_task:
            self._keepalive_task.cancel()
            self._keepalive_task = None

    # --- Authentication ---
    async def _login_by_token_internal(self, token: str):
        if self._session_token != token:
            self._session_token = token

        response = await self.invoke_method(
            opcode=19,
            payload={"interactive": True, "token": token, "chatsSync": 0, "contactsSync": 0,
                     "presenceSync": 0, "draftsSync": 0, "chatsCount": 40}
        )

        payload = response.get("payload", {})
        if payload.get("error"):
            return response

        if payload.get("chats"):
            self._cached_chats = payload["chats"]
            _logger.info(f"Cached {len(payload['chats'])} chats from login")

        if payload.get("contacts"):
            self._cached_contacts = payload["contacts"]
            _logger.info(f"Cached {len(payload['contacts'])} contacts from login")

        chats = payload.get("config", {}).get("chats")
        if chats:
            self._cached_favourite_chats = chats
            _logger.info(f"Cached {len(chats)} favourite chats from login")

        self._is_logged_in = True
        await self._start_keepalive_task()
        return response

    async def _send_hello_packet(self):
        return await self.invoke_method(
            opcode=6,
            payload={
                "userAgent": {
                    "deviceType": "WEB",
                    "locale": "ru",
                    "osVersion": "macOS",
                    "deviceName": "MAX WEB",
                    "headerUserAgent": USER_AGENT,
                    "deviceLocale": "ru-RU",
                    "appVersion": self.app_version,
                    "screen": "956x1470 2.0x",
                    "timezone": "Europe/Moscow"
                },
                "deviceId": self.device_id
            }
        )

    @ensure_connected
    async def send_code(self, phone: str, send_type: str = "RESEND"):
        return await self.invoke_method(opcode=17, payload={"phone": phone, "type": send_type, "language": "ru"})

    @ensure_connected
    async def sign_in(self, sms_token: str, sms_code: int):
        response = await self.invoke_method(opcode=18, payload={
            "token": sms_token,
            "verifyCode": str(sms_code),
            "authTokenType": "CHECK_CODE"
        })
        if "error" not in response.get("payload", {}):
            await self._start_keepalive_task()
        return response

    @ensure_connected
    async def sign_in_password(self, track_id: str, password: str):
        response = await self.invoke_method(opcode=115, payload={"trackId": track_id, "password": password})
        return response

    @ensure_connected
    async def login_by_token(self, token: str):
        if not self._is_logged_in:
            return await self._login_by_token_internal(token)

    async def logout(self):
        if not self._connection or not self._connected:
            _logger.warning("Logout called but no active connection.")
        else:
            try:
                await self._connection.send(
                    json.dumps({"ver": RPC_VERSION, "cmd": 0, "seq": next(self._seq), "opcode": 20})
                )
            except Exception as e:
                _logger.warning(f"Server logout request failed: {e}")

        self._session_token = None
        self._is_logged_in = False
        self._cached_chats = None
        self._cached_contacts = None
        self._cached_favourite_chats = None
        self._pending.clear()
        self._file_pending.clear()
        self._video_pending.clear()

        _logger.info("User logged out, connection still alive.")

    def get_cached_chats(self):
        return self._cached_chats

    def get_cached_contacts(self):
        return self._cached_contacts

    def get_cached_favourite_chats(self):
        return self._cached_favourite_chats
