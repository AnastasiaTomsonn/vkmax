import asyncio
import itertools
import json
import logging
import uuid
from datetime import datetime
from typing import Any, Callable, Optional

import aiohttp
import websockets
from websockets import ConnectionClosedOK, ConnectionClosedError, ConnectionClosed, Close
from websockets.asyncio.client import ClientConnection

from functools import wraps

WS_HOST = "wss://ws-api.oneme.ru/websocket"
RPC_VERSION = 11
APP_VERSION = "25.11.2"
USER_AGENT = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Safari/537.36"

_logger = logging.getLogger(__name__)


def ensure_connected(method: Callable):
    @wraps(method)
    async def wrapper(self, *args, **kwargs):
        if getattr(self, "_closing", False):
            raise RuntimeError("Client is closing.")
        if self._connection is None:
            _logger.warning("Connection is None — reconnecting...")
            self._trigger_reconnect()
            await asyncio.sleep(0.1)

        elif self._connection.state.name in ("CLOSING", "CLOSED"):
            _logger.warning("Connection lost — triggering reconnect...")
            self._trigger_reconnect()
            await asyncio.sleep(0.1)
        return await method(self, *args, **kwargs)

    return wrapper


def handle_errors(method: Callable):
    @wraps(method)
    async def wrapper(self, *args, **kwargs):
        try:
            result = await method(self, *args, **kwargs)
            return {"status": True, "result": result}
        except Exception as e:
            _logger.error(f"{method.__name__} ERROR: {e}")
            return {"status": False, "message": e}

    return wrapper


class MaxClient:
    def __init__(self):
        self.app_version = APP_VERSION
        self._connection: Optional[ClientConnection] = None
        self._http_pool: Optional[aiohttp.ClientSession] = None
        self._is_logged_in: bool = False
        self._seq = itertools.count(1)
        self._keepalive_task: Optional[asyncio.Task] = None
        self._recv_task: Optional[asyncio.Task] = None
        self._incoming_event_callback = None
        self._pending: dict[int, asyncio.Future] = {}

        self._device_id = str(uuid.uuid4())
        self._session_token: Optional[str] = None

        self._video_pending = {}
        self._file_pending = {}
        self._cached_chats = None
        self._cached_contacts = None
        self._cached_favourite_chats = None

        # Locks to prevent concurrent connects/reconnects
        self._connect_lock = asyncio.Lock()
        self._reconnect_lock = asyncio.Lock()

        self._is_reconnecting = False
        self._closing = False

        self.is_test = True

    # --- WebSocket connection management ---
    @handle_errors
    async def connect(self):
        if self._connection:
            try:
                await self._connection.close()
            except Exception:
                pass

        _logger.info(f'Connecting to {WS_HOST}...')
        self._connection = await websockets.connect(
            WS_HOST,
            origin=websockets.Origin('https://web.max.ru'),
            user_agent_header=USER_AGENT
        )

        self._recv_task = asyncio.create_task(self._recv_loop(), name="ws-recv-loop")
        self._keepalive_task = asyncio.create_task(self._keepalive_loop(), name="ws-keepalive")
        _logger.info("Connected and tasks started.")
        return self._connection

    async def _reconnect(self):
        if self._is_reconnecting:
            return

        if self._closing:
            return

        self._is_reconnecting = True
        _logger.warning("Reconnecting WebSocket...")

        # Устанавливаем начальный backoff
        backoff = getattr(self, "_backoff", 3)

        while not self._closing:
            try:
                # Безопасно останавливаем таски и закрываем соединение
                await self.disconnect()
            except Exception as e:
                _logger.error(f"Reconnect: disconnect failed: {e}")

            _logger.info(f"Sleeping before reconnect: {backoff}s")
            await asyncio.sleep(backoff)

            try:
                await self.connect()
                _logger.info("Reconnect: connected")
            except Exception as e:
                _logger.error(f"Reconnect: connect failed: {e}")
                backoff = min(backoff + 2, 30)
                continue

            try:
                await asyncio.wait_for(self._send_hello_packet(), timeout=3)
                _logger.info("Reconnect: hello sent")
            except Exception as e:
                _logger.error(f"Reconnect: hello failed: {e}")
                backoff = min(backoff + 2, 30)
                continue

            if self._is_logged_in and hasattr(self, "_session_token"):
                try:
                    await asyncio.wait_for(self.login_by_token(self._session_token), timeout=5)
                    _logger.info("Reconnect: login successful")
                except Exception as e:
                    _logger.error(f"Reconnect: login failed: {e}")
                    backoff = min(backoff + 2, 30)
                    continue

            _logger.warning("Reconnected successfully.")
            break

        self._is_reconnecting = False

    def _trigger_reconnect(self):
        if not self._is_reconnecting:
            asyncio.create_task(self._reconnect())

    async def disconnect(self):
        self._closing = True
        await self._stop_keepalive_task()
        self._recv_task.cancel()
        if self._connection:
            try:
                await self._connection.close()
            except Exception:
                pass
            self._connection = None
        if self._http_pool:
            await self._http_pool.close()
            self._http_pool = None
        self._closing = False

    @ensure_connected
    async def invoke_method(self, opcode: int, payload: dict[str, Any], retries: int = 2):
        seq = next(self._seq)

        request = {
            "ver": RPC_VERSION,
            "cmd": 0,
            "seq": seq,
            "opcode": opcode,
            "payload": payload
        }
        _logger.info(f'-> REQUEST: {request}')

        future = asyncio.get_event_loop().create_future()
        self._pending[seq] = future

        try:
            await self._connection.send(json.dumps(request))
        except websockets.exceptions.ConnectionClosed:
            _logger.warning('got ws disconnect in invoke_method')
            _logger.info('reconnecting')
            self._trigger_reconnect()
            if retries > 0:
                _logger.info('retrying invoke_method after reconnect')
                await self.invoke_method(opcode, payload, retries - 1)
            return

        try:
            response = await asyncio.wait_for(future, timeout=30)
        except asyncio.TimeoutError:
            await self._pending.pop(seq, None)
            raise TimeoutError("invoke_method timeout waiting for response")
        except Exception as e:
            await self._pending.pop(seq, None)
            raise

        _logger.info(f'<- RESPONSE: {response}')
        return response

    async def set_callback(self, function):
        if not asyncio.iscoroutinefunction(function):
            raise TypeError('callback must be async')
        self._incoming_event_callback = function

    async def _recv_loop(self):
        try:
            async for packet in self._connection:
                try:
                    packet = json.loads(packet)
                except Exception as e:
                    _logger.error(f"JSON decode error in recv loop: {e}")
                    continue

                seq = packet.get("seq")
                if seq is not None:
                    future = self._pending.pop(seq, None)
                    if future and not future.done():
                        future.set_result(packet)
                        continue

                if packet.get("opcode") == 136:
                    payload = packet.get("payload", {})
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
            ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            _logger.info(f"[{ts}] Receiver cancelled")
            return

        except ConnectionClosedOK as e:
            ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            _logger.info(f"[{ts}] WebSocket closed gracefully: {e}")
            return

        except ConnectionClosedError as e:
            ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            _logger.warning(f"[{ts}] ConnectionClosedError ({e}) — reconnecting...")
            self._connection = None
            self._trigger_reconnect()
            return

        except Exception as e:
            ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            _logger.error(f"[{ts}] Recv loop exception ({e}) — reconnecting...")
            self._connection = None
            self._trigger_reconnect()
            return

    # --- Keepalive system
    @ensure_connected
    async def _send_keepalive_packet(self):
        await self.invoke_method(
            opcode=1,
            payload={"interactive": False}
        )

    @ensure_connected
    async def _keepalive_loop(self):
        _logger.info(f'keepalive task started')
        while True:
            try:
                await self._send_keepalive_packet()
                await asyncio.sleep(12)
            except Exception as e:
                ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                _logger.error(f"[{ts}] Keepalive send error ({e}) — reconnecting...")
                self._trigger_reconnect()
                break

    @ensure_connected
    async def _start_keepalive_task(self):
        if self._keepalive_task:
            return
        self._keepalive_task = asyncio.create_task(self._keepalive_loop())
        return

    async def _stop_keepalive_task(self):
        if not self._keepalive_task:
            return
        try:
            self._keepalive_task.cancel()
        except Exception:
            pass
        self._keepalive_task = None
        return

    # --- Authentication ---

    @ensure_connected
    async def _send_hello_packet(self):
        return await self.invoke_method(
            opcode=6,
            payload={
                "userAgent": {
                    "deviceType": "WEB",
                    "locale": "ru_RU",
                    "osVersion": "macOS",
                    "deviceName": "vkmax Python",
                    "headerUserAgent": USER_AGENT,
                    "deviceLocale": "ru-RU",
                    "appVersion": self.app_version,
                    "screen": "956x1470 2.0x",
                    "timezone": "Asia/Vladivostok"
                },
                "deviceId": self._device_id
            }
        )

    @handle_errors
    @ensure_connected
    async def send_code(self, phone: str) -> str:
        """:returns: Login token."""
        await self._send_hello_packet()
        start_auth_response = await self.invoke_method(
            opcode=17,
            payload={
                "phone": phone,
                "type": "START_AUTH",
                "language": "ru"
            }
        )

        if "error" in start_auth_response["payload"]:
            raise Exception(start_auth_response["payload"])

        return start_auth_response["payload"]["token"]

    @handle_errors
    @ensure_connected
    async def sign_in(self, sms_token: str, sms_code: int):
        verification_response = await self.invoke_method(
            opcode=18,
            payload={
                "token": sms_token,
                "verifyCode": str(sms_code),
                "authTokenType": "CHECK_CODE"
            }
        )

        if "error" in verification_response["payload"]:
            raise Exception(verification_response["payload"])

        if "passwordChallenge" in verification_response["payload"].keys():
            return verification_response

        try:
            phone = verification_response["payload"]["profile"]["phone"]
        except Exception:
            phone = '[?]'
            _logger.warning('Got no phone number in server response')
        _logger.info(f'Successfully logged in as {phone}')

        self._is_logged_in = True
        await self._start_keepalive_task()

        return verification_response

    @handle_errors
    @ensure_connected
    async def sing_in_password(self, track_id: str, password: str):
        verification_response = await self.invoke_method(
            opcode=115,
            payload={
                "trackId": track_id,
                "password": password
            }
        )

        if "error" in verification_response["payload"]:
            raise Exception(verification_response["payload"])

        try:
            phone = verification_response["payload"]["profile"]["phone"]
        except Exception:
            phone = '[?]'
            _logger.warning('Got no phone number in server response')
        _logger.info(f'Successfully logged in as {phone}')

        self._is_logged_in = True

        return verification_response

    @handle_errors
    @ensure_connected
    async def login_by_token(self, token: str):
        if token and self._session_token != token:
            self._session_token = token
        await self._send_hello_packet()
        _logger.info("using session")
        login_response = await self.invoke_method(
            opcode=19,
            payload={
                "interactive": True,
                "token": token,
                "chatsSync": 0,
                "contactsSync": 0,
                "presenceSync": 0,
                "draftsSync": 0,
                "chatsCount": 40
            }
        )

        if "error" in login_response["payload"]:
            raise Exception(login_response["payload"])

        try:
            phone = login_response["payload"]["profile"]["phone"]
        except Exception:
            phone = '[?]'
            _logger.warning('Got no phone number in server response')
        _logger.info(f'Successfully logged in as {phone}')

        # Cache chats from login response
        if "chats" in login_response["payload"]:
            self._cached_chats = login_response["payload"]["chats"]
            _logger.info(
                f"Cached {len(login_response['payload']['chats'])} chats from login"
            )

        # Cache chats from login response
        if "contacts" in login_response["payload"]:
            self._cached_contacts = login_response["payload"]["contacts"]
            _logger.info(
                f"Cached {len(login_response['payload']['contacts'])} chats from login"
            )

        if "config" in login_response["payload"]:
            chats = login_response.get("payload", {}).get("config", {}).get("chats")
            if chats:
                self._cached_favourite_chats = chats
                _logger.info(
                    f"Cached {len(chats)} favourite chats from login"
                )

        self._is_logged_in = True
        await self._start_keepalive_task()

        return login_response

    @handle_errors
    async def logout(self):
        if not self._connection:
            _logger.warning("Logout called but no active connection.")
            return

        try:
            try:
                await self.invoke_method(
                    opcode=20,
                    payload={}
                )
            except Exception as e:
                _logger.warning(f"Server logout request failed: {e}")

            if self._keepalive_task:
                await self._stop_keepalive_task()

            await self._connection.close()

            self._connection = None
            self._session_token = None
            self._is_logged_in = False
            self._cached_chats = None
            self._pending.clear()
            self._file_pending.clear()
            self._video_pending.clear()

            _logger.info("Logout successful, connection closed and state cleared.")

        except Exception as e:
            _logger.error(f"LOGOUT_ERROR: {e}")
            raise

    def get_cached_chats(self):
        return self._cached_chats

    def get_cached_contacts(self):
        return self._cached_contacts

    def get_cached_favourite_chats(self):
        return self._cached_favourite_chats

    async def test_force_disconnect(self):
        """Принудительно разрывает текущее соединение."""
        if self._connection is not None:
            try:
                # 1006 — абнормальное закрытие
                self._connection = None
            except Exception:
                pass
