import asyncio
import itertools
import json
import logging
import uuid
from typing import Any, Callable, Optional

import aiohttp
import websockets
from websockets.asyncio.client import ClientConnection

from functools import wraps

WS_HOST = "wss://ws-api.oneme.ru/websocket"
RPC_VERSION = 11
APP_VERSION = "25.9.15"
USER_AGENT = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Safari/537.36"

_logger = logging.getLogger(__name__)


def ensure_connected(method: Callable):
    @wraps(method)
    def wrapper(self, *args, **kwargs):
        if self._connection is None:
            raise RuntimeError("WebSocket not connected. Call .connect() first.")
        return method(self, *args, **kwargs)

    return wrapper


# def handle_errors(method: Callable):
#     @wraps(method)
#     async def wrapper(self, *args, **kwargs):
#         try:
#             result = await method(self, *args, **kwargs)
#             return {"status": True, "result": result}
#         except Exception as e:
#             _logger.error(f"{method.__name__} ERROR: {e}")
#             return {"status": False, "message": e}
#
#     return wrapper


class MaxClient:
    def __init__(self):
        self._connection: Optional[ClientConnection] = None
        self._http_pool: Optional[aiohttp.ClientSession] = None
        self._is_logged_in: bool = False
        self._seq = itertools.count(1)
        self._keepalive_task: Optional[asyncio.Task] = None
        self._recv_task: Optional[asyncio.Task] = None
        self._incoming_event_callback = None
        self._pending = {}
        self._video_pending = {}
        self._file_pending = {}
        self._cached_chats = None
        self._cached_contacts = None

    # --- WebSocket connection management ---
    # @handle_errors
    async def connect(self):
        if self._connection:
            return self._connection

        _logger.info(f'Connecting to {WS_HOST}...')
        self._connection = await websockets.connect(
            WS_HOST,
            origin=websockets.Origin('https://web.max.ru'),
            user_agent_header=USER_AGENT
        )

        self._recv_task = asyncio.create_task(self._recv_loop())
        _logger.info('Connected. Receive task started.')
        return self._connection

    @ensure_connected
    async def disconnect(self):
        await self._stop_keepalive_task()
        self._recv_task.cancel()
        await self._connection.close()
        if self._http_pool:
            await self._http_pool.close()

    @ensure_connected
    async def invoke_method(self, opcode: int, payload: dict[str, Any]):
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

        await self._connection.send(
            json.dumps(request)
        )

        response = await future
        _logger.info(f'<- RESPONSE: {response}')

        return response

    async def set_callback(self, function):
        if not asyncio.iscoroutinefunction(function):
            raise TypeError('callback must be async')
        self._incoming_event_callback = function

    async def _recv_loop(self):
        try:
            async for packet in self._connection:
                packet = json.loads(packet)

                seq = packet["seq"]
                future = self._pending.pop(seq, None)
                if future:
                    future.set_result(packet)
                    continue

                if packet.get("opcode") == 136:
                    payload = packet.get("payload", {})
                    future = None

                    if "videoId" in payload:
                        future = self._video_pending.pop(payload["videoId"], None)
                    elif "fileId" in payload:
                        future = self._file_pending.pop(payload["fileId"], None)

                    if future:
                        future.set_result(None)

                if self._incoming_event_callback:
                    asyncio.create_task(self._incoming_event_callback(self, packet))

        except asyncio.CancelledError:
            _logger.info(f'receiver cancelled')
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
        try:
            while True:
                await self._send_keepalive_packet()
                await asyncio.sleep(30)
        except asyncio.CancelledError:
            _logger.info('keepalive task stopped')
            return

    @ensure_connected
    async def _start_keepalive_task(self):
        if self._keepalive_task:
            return

        self._keepalive_task = asyncio.create_task(self._keepalive_loop())
        return

    async def _stop_keepalive_task(self):
        if not self._keepalive_task:
            return

        self._keepalive_task.cancel()
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
                    "appVersion": APP_VERSION,
                    "screen": "956x1470 2.0x",
                    "timezone": "Asia/Vladivostok"
                },
                "deviceId": str(uuid.uuid4())
            }
        )

    # @handle_errors
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

    # @handle_errors
    @ensure_connected
    async def sign_in(self, sms_token: str, sms_code: int):
        """
        Auth token for further login is at ['payload']['tokenAttrs']['LOGIN']['token']
        :param login_token: Must be obtained via `send_code`.
        """
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
        except:
            phone = '[?]'
            _logger.warning('Got no phone number in server response')
        _logger.info(f'Successfully logged in as {phone}')

        self._is_logged_in = True
        await self._start_keepalive_task()

        return verification_response

    # @handle_errors
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
        except:
            phone = '[?]'
            _logger.warning('Got no phone number in server response')
        _logger.info(f'Successfully logged in as {phone}')

        self._is_logged_in = True

        return verification_response

    # @handle_errors
    @ensure_connected
    async def login_by_token(self, token: str):
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
        except:
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

        self._is_logged_in = True
        await self._start_keepalive_task()

        return login_response

    # @handle_errors
    @ensure_connected
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
            self._is_logged_in = False
            self._cached_chats = None
            self._pending.clear()

            _logger.info("Logout successful, connection closed and state cleared.")

        except Exception as e:
            _logger.error(f"LOGOUT_ERROR: {e}")
            raise

    def get_cached_chats(self):
        """
        Get chats that were cached during login.
        Returns the full login response containing chats, or None if not available.
        """
        return self._cached_chats

    def get_cached_contacts(self):
        """
        Get chats that were cached during login.
        Returns the full login response containing chats, or None if not available.
        """
        return self._cached_contacts
