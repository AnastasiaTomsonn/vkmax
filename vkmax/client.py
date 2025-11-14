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
USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/137.0.0.0 Safari/537.36"
)

_logger = logging.getLogger(__name__)


# --- Decorators ------------------------------------------------------

def ensure_connected(method: Callable):
    @wraps(method)
    def wrapper(self, *args, **kwargs):
        if self._connection is None:
            raise RuntimeError("WebSocket not connected. Call .connect() first.")
        return method(self, *args, **kwargs)

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


# --- Client ----------------------------------------------------------

class MaxClient:
    def __init__(self):
        # Connection and session state
        self._connection: Optional[ClientConnection] = None
        self._http_pool: Optional[aiohttp.ClientSession] = None

        # Auth & state
        self._is_logged_in: bool = False
        self._seq = itertools.count(1)

        # Tasks
        self._keepalive_task: Optional[asyncio.Task] = None
        self._recv_task: Optional[asyncio.Task] = None

        # Callbacks
        self._incoming_event_callback = None

        # Pending RPC calls
        self._pending: dict[int, asyncio.Future] = {}
        self._video_pending: dict[str, asyncio.Future] = {}
        self._file_pending: dict[str, asyncio.Future] = {}

        # Cached data
        self._cached_chats = None
        self._cached_favourite_chats = None
        self._cached_contacts = None

        # Control for start() loop
        self._running = False
        self._reconnect_delay = 3.0

    # =================================================================
    # New / public helpers
    # =================================================================

    async def start(self):
        """
        Запускает вечный цикл подключения с авто-переподключением.
        Внешний код должен вызывать именно этот метод.
        """
        if self._running:
            _logger.info("MaxClient.start: already running")
            return

        self._running = True
        _logger.info("MaxClient: start loop initiated")

        while self._running:
            try:
                # connect() возвращает структуру в обёртке handle_errors,
                # но внутри он создаёт self._connection и _recv_task
                await self.connect()
                _logger.info("MaxClient: connection established, waiting for recv task to finish")
                # ждём пока recv-loop завершится (внезапный разрыв/закрытие)
                if self._recv_task:
                    try:
                        await self._recv_task
                    except asyncio.CancelledError:
                        _logger.info("MaxClient: recv task cancelled while awaiting")
                else:
                    # на случай, если connect не создал recv_task (маловероятно)
                    await asyncio.sleep(self._reconnect_delay)

            except Exception as e:
                _logger.exception(f"MaxClient.start: unexpected error: {e}")

            # Очистка состояния перед переподключением
            try:
                # остановим keepalive если он ещё висит
                await self._stop_keepalive_task()
            except Exception:
                pass

            # Помним: не чистим cached_* и auth состояние — это делает logout()
            # Очистим временные структуры, которые могли быть привязаны к соединению
            self._connection = None
            # не удаляем _pending полностью — это может привести к залипшим future;
            # но если соединение оборвано, лучше отменить все ожидания
            for fut in list(self._pending.values()):
                if not fut.done():
                    fut.set_exception(RuntimeError("Connection lost"))
            self._pending.clear()

            _logger.info(f"MaxClient: reconnecting in {self._reconnect_delay} seconds...")
            await asyncio.sleep(self._reconnect_delay)

        _logger.info("MaxClient: start loop finished (stopped)")

    async def stop(self):
        """Останавливает start loop и корректно закрывает соединение."""
        self._running = False
        if self._recv_task:
            self._recv_task.cancel()
        await self.disconnect()

    async def send_raw(self, payload: dict):
        """
        Низкоуровневая отправка произвольного JSON в сокет.
        Используй для нестандартных операций (не RPC).
        """
        if not self._connection:
            raise RuntimeError("WebSocket not connected. Call .connect() first.")
        await self._connection.send(json.dumps(payload))

    def set_attribute(self, handler: Callable):
        """
        Альтернативное имя для установки обработчика входящих событий.
        Поддерживает как async def handler(...), так и sync def handler(...).
        Внешний проект использует set_attribute — делаем совместимость.
        """
        # Если передали coroutine function — сохраняем напрямую
        if asyncio.iscoroutinefunction(handler):
            self._incoming_event_callback = handler
            return

        # Если передан sync function — завернём в coroutine
        def _wrap_sync(*args, **kwargs):
            return handler(*args, **kwargs)

        async def _async_wrapper(*args, **kwargs):
            return _wrap_sync(*args, **kwargs)

        self._incoming_event_callback = _async_wrapper

    # Сохраняем существующий API (чтобы ничего не ломать)
    async def set_callback(self, function):
        if not asyncio.iscoroutinefunction(function):
            raise TypeError('callback must be async')
        self._incoming_event_callback = function

    # =================================================================
    # WebSocket Connection
    # =================================================================
    @handle_errors
    async def connect(self):
        """
        Устанавливает WebSocket-соединение (один раз).
        Возвращает результат в виде dict от handle_errors.
        """
        if self._connection:
            _logger.debug("connect(): already connected")
            return self._connection

        _logger.info(f'Connecting to {WS_HOST}...')
        # используем websockets.connect как раньше — совместимость сохранена
        self._connection = await websockets.connect(
            WS_HOST,
            origin=websockets.Origin('https://web.max.ru'),
            user_agent_header=USER_AGENT
        )

        if self._is_logged_in:
            await self._start_keepalive_task()

        # стартуем recv loop
        self._recv_task = asyncio.create_task(self._recv_loop())
        _logger.info('Connected. Receive task started.')
        return self._connection

    @ensure_connected
    async def disconnect(self):
        """Корректно закрывает соединение и http пул."""
        try:
            await self._stop_keepalive_task()
        except Exception:
            _logger.exception("Error while stopping keepalive")

        if self._recv_task:
            # аккуратно остановим recv_task
            self._recv_task.cancel()
            try:
                await self._recv_task
            except asyncio.CancelledError:
                _logger.debug("recv_task cancelled on disconnect")

        try:
            await self._connection.close()
        except Exception:
            _logger.exception("Error closing websocket connection")

        self._connection = None

        if self._http_pool:
            try:
                await self._http_pool.close()
            except Exception:
                _logger.exception("Error closing http pool")
            self._http_pool = None

    @ensure_connected
    async def invoke_method(self, opcode: int, payload: dict[str, Any]):
        """
        RPC-вызов: отправляет запрос и ждёт ответа, использует seq.
        Возвращает распарсенный ответ.
        """
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

        # отправляем JSON
        await self._connection.send(json.dumps(request))

        # ждём ответа (future будет установлен в _handle_packet)
        response = await future
        _logger.info(f'<- RESPONSE: {response}')

        return response

    # =================================================================
    # Receiving messages
    # =================================================================
    async def _recv_loop(self):
        try:
            async for packet_raw in self._connection:
                try:
                    packet = json.loads(packet_raw)
                except Exception:
                    _logger.warning(f"Malformed packet: {packet_raw}")
                    continue
                await self._handle_packet(packet)

        except websockets.ConnectionClosedError as e:
            _logger.warning(f"_recv_loop: connection closed unexpectedly: {e}")

        except asyncio.CancelledError:
            _logger.info('recv_loop cancelled')
            return

        except Exception as e:
            _logger.exception(f"_recv_loop error: {e}")

        finally:
            _logger.warning("recv loop ended (connection dropped)")

    async def _handle_packet(self, packet: dict):
        """
        Вынесенная логика обработки одного пакета:
         - ответ на invoke_method (по seq)
         - обработка attachment (opcode==136)
         - вызов внешнего callback для событий
        """
        seq = packet.get("seq")
        if seq is not None:
            future = self._pending.pop(seq, None)
            if future:
                # устанавливаем результат для ожидающего invoke_method
                if not future.done():
                    future.set_result(packet)
                return

        # special-case: attachment/video/file progress (opcode 136 как прежде)
        if packet.get("opcode") == 136:
            payload = packet.get("payload", {}) or {}
            future = None

            vid = payload.get("videoId")
            fid = payload.get("fileId")

            if vid:
                future = self._video_pending.pop(vid, None)
            elif fid:
                future = self._file_pending.pop(fid, None)

            if future and not future.done():
                future.set_result(None)
                return

        # incoming event - делегируем внешнему обработчику (если есть)
        if self._incoming_event_callback:
            try:
                # запускаем в фоне, чтобы не блокировать recv loop
                asyncio.create_task(self._incoming_event_callback(self, packet))
            except Exception:
                _logger.exception("Error scheduling incoming event callback")

    # --- Keepalive system ------------------------------------------------

    @ensure_connected
    async def _send_keepalive_packet(self):
        await self.invoke_method(
            opcode=1,
            payload={"interactive": False}
        )

    @ensure_connected
    async def _keepalive_loop(self):
        _logger.info('keepalive task started')
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
        try:
            await self._keepalive_task
        except asyncio.CancelledError:
            pass
        self._keepalive_task = None
        return

    # --- Authentication --------------------------------------------------

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

        if "chats" in login_response["payload"]:
            self._cached_chats = login_response["payload"]["chats"]
            _logger.info(
                f"Cached {len(login_response['payload']['chats'])} chats from login"
            )

        if "contacts" in login_response["payload"]:
            self._cached_contacts = login_response["payload"]["contacts"]
            _logger.info(
                f"Cached {len(login_response['payload']['contacts'])} contacts from login"
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
            self._video_pending.clear()
            self._file_pending.clear()

            _logger.info("Logout successful, connection closed and state cleared.")

        except Exception as e:
            _logger.error(f"LOGOUT_ERROR: {e}")
            raise

    # =================================================================
    # Cached data access
    # =================================================================

    def get_cached_chats(self):
        return self._cached_chats

    def get_cached_contacts(self):
        return self._cached_contacts

    def get_favourite_chats(self):
        return self._cached_favourite_chats
