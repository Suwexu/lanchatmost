import asyncio
import json
import logging
import websockets
from typing import Optional, Dict, Any, Callable, List

logger = logging.getLogger(__name__)


class LanChatClient:
    """Клиент для работы с LanChat API через WebSocket"""
    
    def __init__(self, token: str, ws_url: str = "wss://msgpublic.langame.ru/wsapi", 
                 api_url: str = "https://msgpublic.langame.ru"):
        self.token = token
        self.ws_url = ws_url
        self.api_url = api_url
        self.websocket: Optional[websockets.WebSocketClientProtocol] = None
        self.subscribed_chats = set()
        self.message_handlers = []
        self._running = False
        self.chats_available = []
        self._chats_received = asyncio.Event()
    
    def on_message(self, handler: Callable):
        self.message_handlers.append(handler)
        return handler
    
    async def connect(self):
        try:
            uri = f"{self.ws_url}?token={self.token}"
            logger.info(f"Подключение к WebSocket: {self.ws_url}")
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
            logger.error(f"Ошибка подключения: {e}")
            return False
    
    async def authenticate(self):
        if not self.websocket:
            return False
        try:
            auth_frame = json.dumps({"t": "auth", "token": self.token})
            await self.websocket.send(auth_frame)
            logger.info("🔑 Auth отправлен")
            return True
        except Exception as e:
            logger.error(f"Ошибка auth: {e}")
            return False
    
    async def subscribe(self, chat_id: str):
        if not self.websocket:
            return False
        try:
            frame = json.dumps({"t": "subscribe", "chatId": chat_id})
            await self.websocket.send(frame)
            self.subscribed_chats.add(chat_id)
            logger.info(f"📡 Подписка на {chat_id}")
            return True
        except Exception as e:
            logger.error(f"Ошибка подписки: {e}")
            return False
    
    async def get_available_chats(self) -> List[Dict[str, Any]]:
        self._chats_received.clear()
        try:
            await asyncio.wait_for(self._chats_received.wait(), timeout=10.0)
            return self.chats_available
        except asyncio.TimeoutError:
            logger.warning("Таймаут получения чатов")
            return []
    
    async def listen(self):
        if not self.websocket:
            return
        
        self._running = True
        
        try:
            async for raw in self.websocket:
                try:
                    msg = json.loads(raw)
                    t = msg.get("t")
                    
                    if t == "authed":
                        logger.info("✅ Авторизация успешна")
                        for chat_id in self.subscribed_chats:
                            await self.subscribe(chat_id)
                    
                    elif t == "chats_available":
                        self.chats_available = msg.get("chats", [])
                        self._chats_received.set()
                        logger.info(f"📋 Получено {len(self.chats_available)} чатов")
                    
                    elif t == "subscribed":
                        logger.info(f"✅ Подписка подтверждена: {msg.get('chatId')}")
                    
                    elif t == "message_new":
                        chat_id = msg.get("chatId")
                        msg_data = msg.get("message", {})
                        for handler in self.message_handlers:
                            try:
                                if asyncio.iscoroutinefunction(handler):
                                    await handler(chat_id, msg_data)
                                else:
                                    handler(chat_id, msg_data)
                            except Exception as e:
                                logger.error(f"Ошибка в обработчике: {e}")
                    
                    elif t == "error":
                        logger.error(f"❌ Ошибка: {msg.get('code')} - {msg.get('message')}")
                        if msg.get("code") in ["unauthorized", "auth_failed"]:
                            logger.critical("❌ Ошибка авторизации!")
                            break
                    
                except json.JSONDecodeError as e:
                    logger.error(f"Ошибка JSON: {e}")
                    
        except websockets.exceptions.ConnectionClosed as e:
            logger.warning(f"Соединение закрыто: {e}")
            self._running = False
        except Exception as e:
            logger.error(f"Ошибка: {e}")
            self._running = False
    
    async def close(self):
        self._running = False
        if self.websocket:
            await self.websocket.close()