import asyncio
import json
import logging
import msgpack
import websockets
from typing import Optional, Dict, Any, Callable, List
from tenacity import retry, stop_after_attempt, wait_exponential
import aiohttp

logger = logging.getLogger(__name__)


class LanChatClient:
    """Клиент для работы с LanChat API через WebSocket и HTTP"""
    
    def __init__(self, token: str, ws_url: str, api_url: str = "https://msgpublic.langame.ru"):
        self.token = token
        self.ws_url = ws_url
        self.api_url = api_url
        self.websocket: Optional[websockets.WebSocketClientProtocol] = None
        self.subscribed_chats = set()
        self.message_handlers = []
        self._running = False
    
    def on_message(self, handler: Callable):
        """Декоратор для регистрации обработчика сообщений"""
        self.message_handlers.append(handler)
        return handler
    
    async def connect(self):
        """Подключение к WebSocket"""
        uri = f"{self.ws_url}?token={self.token}"
        
        @retry(
            stop=stop_after_attempt(5),
            wait=wait_exponential(multiplier=1, min=2, max=30)
        )
        async def connect_with_retry():
            try:
                logger.info(f"Подключение к LanChat WebSocket: {self.ws_url}")
                self.websocket = await websockets.connect(
                    uri,
                    ping_interval=20,
                    ping_timeout=30,
                    max_size=10 * 1024 * 1024  # 10MB
                )
                logger.info("WebSocket подключен успешно")
                
                auth_frame = msgpack.packb({"t": "auth", "token": self.token})
                await self.websocket.send(auth_frame)
                
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
    
    async def subscribe(self, chat_id: str):
        """Подписка на чат"""
        if not self.websocket:
            logger.error("WebSocket не подключен")
            return False
        
        try:
            subscribe_frame = msgpack.packb({
                "t": "subscribe",
                "chatId": chat_id
            })
            await self.websocket.send(subscribe_frame)
            self.subscribed_chats.add(chat_id)
            logger.info(f"Подписка на чат {chat_id} отправлена")
            return True
        except Exception as e:
            logger.error(f"Ошибка подписки: {e}")
            return False
    
    async def listen(self):
        """Прослушивание входящих сообщений"""
        if not self.websocket:
            logger.error("WebSocket не подключен")
            return
        
        self._running = True
        
        try:
            async for raw_message in self.websocket:
                try:
                    message = msgpack.unpackb(raw_message, raw=False)
                    msg_type = message.get("t")
                    
                    if msg_type == "authed":
                        logger.info("✅ Авторизация в LanChat успешна")
                        for chat_id in self.subscribed_chats:
                            await self.subscribe(chat_id)
                    
                    elif msg_type == "chats_available":
                        logger.info(f"Доступные чаты: {len(message.get('chats', []))}")
                    
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
                    
                    elif msg_type == "error":
                        error_code = message.get("code")
                        error_msg = message.get("message", "Unknown error")
                        logger.error(f"Ошибка LanChat: {error_code} - {error_msg}")
                        
                        if error_code in ["unauthorized", "auth_failed"]:
                            logger.critical("❌ Ошибка авторизации! Проверьте токен")
                            break
                    
                except msgpack.exceptions.UnpackException as e:
                    logger.error(f"Ошибка распаковки MessagePack: {e}")
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
        """Переподключение при обрыве"""
        if self._running:
            logger.info("Попытка переподключения через 5 секунд...")
            await asyncio.sleep(5)
            
            if await self.connect():
                for chat_id in self.subscribed_chats:
                    await self.subscribe(chat_id)
                asyncio.create_task(self.listen())
            else:
                logger.error("Не удалось переподключиться")
    
    async def get_available_chats(self) -> List[Dict[str, Any]]:
        """Получение списка доступных чатов"""
        url = f"{self.api_url}/api/public/chats"
        headers = {"Authorization": f"Bearer {self.token}"}
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, headers=headers) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        chats = data.get("chats", data.get("data", []))
                        logger.info(f"📋 Получено {len(chats)} доступных чатов")
                        return chats
                    else:
                        error = await resp.text()
                        logger.error(f"Ошибка получения чатов: {resp.status} - {error}")
                        return []
        except Exception as e:
            logger.error(f"Ошибка: {e}")
            return []
    
    async def find_chat_by_title(self, title: str) -> Optional[Dict[str, Any]]:
        """Поиск чата по названию"""
        chats = await self.get_available_chats()
        for chat in chats:
            chat_title = chat.get("title", "")
            if title.lower() in chat_title.lower():
                return chat
        return None
    
    async def get_chat_info(self, chat_id: str) -> Optional[Dict[str, Any]]:
        """Получение информации о чате"""
        url = f"{self.api_url}/api/public/chats/{chat_id}"
        headers = {"Authorization": f"Bearer {self.token}"}
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, headers=headers) as resp:
                    if resp.status == 200:
                        return await resp.json()
                    else:
                        logger.error(f"Ошибка получения чата {chat_id}: {resp.status}")
                        return None
        except Exception as e:
            logger.error(f"Ошибка: {e}")
            return None
    
    async def close(self):
        """Закрытие соединения"""
        self._running = False
        if self.websocket:
            await self.websocket.close()
            logger.info("WebSocket соединение закрыто")