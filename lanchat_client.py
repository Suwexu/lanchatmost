import asyncio
import json
import logging
import websockets
from typing import Optional, Dict, Any, Callable, List
from tenacity import retry, stop_after_attempt, wait_exponential
import aiohttp

logger = logging.getLogger(__name__)


class LanChatClient:
    """Клиент для работы с LanChat API через WebSocket + HTTP fallback"""
    
    def __init__(self, token: str, ws_url: str = "wss://msgpublic.langame.ru/wsapi", 
                 api_url: str = "https://msgpublic.langame.ru"):
        self.token = token
        self.ws_url = ws_url
        self.api_url = api_url
        self.websocket: Optional[websockets.WebSocketClientProtocol] = None
        self.subscribed_chats = set()
        self.message_handlers = []
        self._running = False
        self._reconnect_task = None
        self.chats_available = []  # Список доступных чатов
        self._chats_received = asyncio.Event()
    
    def on_message(self, handler: Callable):
        """Регистрация обработчика сообщений"""
        self.message_handlers.append(handler)
        return handler
    
    async def connect(self):
        """Подключение к WebSocket"""
        uri = f"{self.ws_url}?token={self.token}"
        
        @retry(
            stop=stop_after_attempt(3),
            wait=wait_exponential(multiplier=2, min=4, max=30)
        )
        async def connect_with_retry():
            try:
                logger.info(f"Подключение к LanChat WebSocket: {self.ws_url}")
                self.websocket = await websockets.connect(
                    uri,
                    ping_interval=20,
                    ping_timeout=60,
                    close_timeout=30,
                    max_size=10 * 1024 * 1024
                )
                logger.info("✅ WebSocket подключен")
                return True
            except Exception as e:
                logger.error(f"Ошибка подключения к WebSocket: {e}")
                raise
        
        try:
            await connect_with_retry()
            return True
        except Exception as e:
            logger.error(f"Не удалось подключиться: {e}")
            return False
    
    async def authenticate(self):
        """Отправка auth кадра"""
        if not self.websocket:
            logger.error("WebSocket не подключен")
            return False
        
        try:
            auth_frame = json.dumps({"t": "auth", "token": self.token})
            await self.websocket.send(auth_frame)
            logger.info("🔑 Auth кадр отправлен")
            return True
        except Exception as e:
            logger.error(f"Ошибка авторизации: {e}")
            return False
    
    async def subscribe(self, chat_id: str):
        """Подписка на чат"""
        if not self.websocket:
            logger.error("WebSocket не подключен")
            return False
        
        try:
            subscribe_frame = json.dumps({
                "t": "subscribe",
                "chatId": chat_id
            })
            await self.websocket.send(subscribe_frame)
            self.subscribed_chats.add(chat_id)
            logger.info(f"📡 Подписка на чат {chat_id} отправлена")
            return True
        except Exception as e:
            logger.error(f"Ошибка подписки: {e}")
            return False
    
    async def get_available_chats(self) -> List[Dict[str, Any]]:
        """Получение списка доступных чатов через WebSocket"""
        self._chats_received.clear()
        
        if not self.websocket or not self._running:
            logger.warning("WebSocket не активен, пытаемся подключиться...")
            if not await self.connect():
                logger.error("Не удалось подключиться к WebSocket")
                return []
            await self.authenticate()
        
        # Ждем получения списка чатов (максимум 10 секунд)
        try:
            await asyncio.wait_for(self._chats_received.wait(), timeout=10.0)
            return self.chats_available
        except asyncio.TimeoutError:
            logger.warning("Таймаут ожидания списка чатов")
            return []
    
    async def listen(self):
        """Прослушивание WebSocket"""
        if not self.websocket:
            logger.error("WebSocket не подключен")
            return
        
        self._running = True
        
        try:
            async for raw_message in self.websocket:
                try:
                    message = json.loads(raw_message)
                    msg_type = message.get("t")
                    
                    if msg_type == "authed":
                        logger.info("✅ Авторизация в LanChat успешна")
                        # После авторизации подписываемся на чаты
                        for chat_id in self.subscribed_chats:
                            await self.subscribe(chat_id)
                    
                    elif msg_type == "chats_available":
                        chats = message.get("chats", [])
                        self.chats_available = chats
                        self._chats_received.set()
                        logger.info(f"📋 Получено {len(chats)} доступных чатов")
                        for chat in chats:
                            logger.info(f"  - {chat.get('title')} (ID: {chat.get('id')})")
                    
                    elif msg_type == "subscribed":
                        chat_id = message.get("chatId")
                        logger.info(f"✅ Подписка на чат {chat_id} подтверждена")
                    
                    elif msg_type == "message_new":
                        chat_id = message.get("chatId")
                        msg_data = message.get("message", {})
                        
                        for handler in self.message_handlers:
                            try:
                                if asyncio.iscoroutinefunction(handler):
                                    await handler(chat_id, msg_data)
                                else:
                                    handler(chat_id, msg_data)
                            except Exception as e:
                                logger.error(f"Ошибка в обработчике: {e}")
                    
                    elif msg_type == "message_update":
                        logger.debug("Обновление сообщения")
                    
                    elif msg_type == "error":
                        error_code = message.get("code")
                        error_msg = message.get("message", "Unknown error")
                        logger.error(f"❌ Ошибка LanChat: {error_code} - {error_msg}")
                        
                        if error_code in ["unauthorized", "auth_failed"]:
                            logger.critical("❌ Ошибка авторизации! Проверьте токен")
                            break
                    
                    elif msg_type == "pong":
                        logger.debug("🏓 Pong получен")
                    
                    else:
                        logger.debug(f"Получен кадр типа {msg_type}")
                        
                except json.JSONDecodeError as e:
                    logger.error(f"Ошибка парсинга JSON: {e}")
                except Exception as e:
                    logger.error(f"Ошибка обработки сообщения: {e}")
                    
        except websockets.exceptions.ConnectionClosed as e:
            logger.warning(f"WebSocket соединение закрыто: {e}")
            self._running = False
            await self._reconnect()
        except Exception as e:
            logger.error(f"Ошибка в listen: {e}")
            self._running = False
    
    async def _reconnect(self):
        """Переподключение"""
        if self._running:
            logger.info("🔄 Попытка переподключения через 5 секунд...")
            await asyncio.sleep(5)
            
            if await self.connect():
                await self.authenticate()
                for chat_id in self.subscribed_chats:
                    await self.subscribe(chat_id)
                asyncio.create_task(self.listen())
            else:
                logger.error("Не удалось переподключиться")
    
    async def close(self):
        """Закрытие соединения"""
        self._running = False
        if self.websocket:
            await self.websocket.close()
            logger.info("WebSocket соединение закрыто")