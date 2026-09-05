import asyncio
import json
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
        self.last_message_ids = {}  # ID последнего сообщения для каждого чата
        self.poll_interval = 3
        self._session: Optional[aiohttp.ClientSession] = None
    
    def on_message(self, handler: Callable):
        """Регистрация обработчика сообщений"""
        self.message_handlers.append(handler)
        return handler
    
    async def subscribe(self, chat_id: str):
        """Подписка на чат"""
        self.subscribed_chats.add(chat_id)
        if chat_id not in self.last_message_ids:
            self.last_message_ids[chat_id] = None
        logger.info(f"✅ Подписка на чат {chat_id} выполнена")
        return True
    
    async def _get_session(self) -> aiohttp.ClientSession:
        """Получение сессии с таймаутами"""
        if self._session is None or self._session.closed:
            timeout = aiohttp.ClientTimeout(
                total=30,
                connect=10,
                sock_read=20
            )
            self._session = aiohttp.ClientSession(timeout=timeout)
        return self._session
    
    async def listen(self):
        """Прослушивание новых сообщений через HTTP polling"""
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
        """Получение сообщений из чата"""
        url = f"{self.api_url}/api/public/chats/{chat_id}/messages"
        headers = {"Authorization": f"Bearer {self.token}"}
        params = {"limit": 20}
        
        try:
            session = await self._get_session()
            async with session.get(url, headers=headers, params=params) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    messages = data.get("messages", data.get("items", data.get("list", [])))
                    return messages
                else:
                    return []
        except Exception as e:
            logger.error(f"Ошибка получения сообщений: {e}")
            return []
    
    async def get_available_chats(self) -> List[Dict[str, Any]]:
        """Получение списка доступных чатов"""
        url = f"{self.api_url}/api/public/chats"
        headers = {"Authorization": f"Bearer {self.token}"}
        
        try:
            session = await self._get_session()
            async with session.get(url, headers=headers) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    chats = data.get("chats", data.get("data", data.get("items", [])))
                    logger.info(f"📋 Получено {len(chats)} доступных чатов")
                    return chats
                else:
                    logger.error(f"Ошибка получения чатов: {resp.status}")
                    return []
        except Exception as e:
            logger.error(f"Ошибка: {e}")
            return []
    
    async def get_chat_info(self, chat_id: str) -> Optional[Dict[str, Any]]:
        """Получение информации о чате"""
        url = f"{self.api_url}/api/public/chats/{chat_id}"
        headers = {"Authorization": f"Bearer {self.token}"}
        
        try:
            session = await self._get_session()
            async with session.get(url, headers=headers) as resp:
                if resp.status == 200:
                    return await resp.json()
                else:
                    return None
        except Exception as e:
            logger.error(f"Ошибка: {e}")
            return None
    
    async def close(self):
        """Остановка клиента"""
        self._running = False
        self._stop_polling = True
        if self._session and not self._session.closed:
            await self._session.close()
        logger.info("Клиент остановлен")