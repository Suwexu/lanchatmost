import asyncio
import logging
from typing import Optional, Dict, Any, Callable, List
import aiohttp

logger = logging.getLogger(__name__)


class LanChatClient:
    """Клиент для работы с LanChat API через HTTP Polling"""
    
    def __init__(self, token: str, api_url: str = "https://msgpublic.langame.ru"):
        self.token = token
        self.api_url = api_url
        self.subscribed_chats = set()
        self.message_handlers = []
        self._running = False
        self._stop_polling = False
        self.last_message_ids = {}
        self.poll_interval = 5  # Увеличен до 5 секунд
        self._session: Optional[aiohttp.ClientSession] = None
        self._retry_count = 3
    
    def on_message(self, handler: Callable):
        self.message_handlers.append(handler)
        return handler
    
    async def subscribe(self, chat_id: str):
        self.subscribed_chats.add(chat_id)
        if chat_id not in self.last_message_ids:
            self.last_message_ids[chat_id] = None
        logger.info(f"✅ Подписка на чат {chat_id} выполнена")
        return True
    
    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            # Увеличенные таймауты
            timeout = aiohttp.ClientTimeout(
                total=45,
                connect=15,
                sock_read=30
            )
            self._session = aiohttp.ClientSession(timeout=timeout)
        return self._session
    
    async def _make_request(self, method: str, url: str, **kwargs) -> Optional[dict]:
        """Выполнение запроса с повторными попытками"""
        for attempt in range(self._retry_count):
            try:
                session = await self._get_session()
                headers = kwargs.pop("headers", {})
                headers["Authorization"] = f"Bearer {self.token}"
                
                async with session.request(method, url, headers=headers, **kwargs) as resp:
                    if resp.status == 200:
                        return await resp.json()
                    elif resp.status in [401, 403]:
                        logger.error(f"Ошибка авторизации: {resp.status}")
                        return None
                    else:
                        logger.warning(f"Ошибка {resp.status}, попытка {attempt+1}/{self._retry_count}")
                        if attempt < self._retry_count - 1:
                            await asyncio.sleep(2 ** attempt)  # Экспоненциальная задержка
                        continue
            except Exception as e:
                logger.warning(f"Ошибка запроса, попытка {attempt+1}/{self._retry_count}: {e}")
                if attempt < self._retry_count - 1:
                    await asyncio.sleep(2 ** attempt)
                continue
        
        logger.error(f"Не удалось выполнить запрос после {self._retry_count} попыток")
        return None
    
    async def listen(self):
        if not self.subscribed_chats:
            logger.error("Нет подписанных чатов")
            return
        
        self._running = True
        self._stop_polling = False
        
        logger.info(f"📡 Запуск HTTP Polling (интервал: {self.poll_interval} сек)")
        
        while self._running and not self._stop_polling:
            try:
                for chat_id in list(self.subscribed_chats):
                    if self._stop_polling:
                        break
                    
                    messages = await self.get_messages(chat_id)
                    
                    if messages:
                        for msg_data in messages:
                            msg_id = msg_data.get("id")
                            if msg_id:
                                last_id = self.last_message_ids.get(chat_id)
                                if last_id and msg_id <= last_id:
                                    continue
                                
                                if not last_id or msg_id > last_id:
                                    self.last_message_ids[chat_id] = msg_id
                                
                                for handler in self.message_handlers:
                                    try:
                                        if asyncio.iscoroutinefunction(handler):
                                            await handler(chat_id, msg_data)
                                        else:
                                            handler(chat_id, msg_data)
                                    except Exception as e:
                                        logger.error(f"Ошибка в обработчике: {e}")
                
                await asyncio.sleep(self.poll_interval)
                
            except Exception as e:
                logger.error(f"Ошибка в polling: {e}")
                await asyncio.sleep(self.poll_interval * 2)
    
    async def get_messages(self, chat_id: str) -> List[Dict[str, Any]]:
        url = f"{self.api_url}/api/public/chats/{chat_id}/messages"
        params = {"limit": 20}
        
        try:
            data = await self._make_request("GET", url, params=params)
            if data:
                return data.get("messages", data.get("items", data.get("list", [])))
            return []
        except Exception as e:
            logger.error(f"Ошибка получения сообщений: {e}")
            return []
    
    async def get_available_chats(self) -> List[Dict[str, Any]]:
        url = f"{self.api_url}/api/public/chats"
        
        try:
            data = await self._make_request("GET", url)
            if data:
                chats = data.get("chats", data.get("data", data.get("items", [])))
                logger.info(f"📋 Получено {len(chats)} доступных чатов")
                return chats
            return []
        except Exception as e:
            logger.error(f"Ошибка получения чатов: {e}")
            return []
    
    async def get_chat_info(self, chat_id: str) -> Optional[Dict[str, Any]]:
        url = f"{self.api_url}/api/public/chats/{chat_id}"
        
        try:
            return await self._make_request("GET", url)
        except Exception as e:
            logger.error(f"Ошибка получения чата: {e}")
            return None
    
    async def close(self):
        self._running = False
        self._stop_polling = True
        if self._session and not self._session.closed:
            await self._session.close()
        logger.info("Клиент остановлен")