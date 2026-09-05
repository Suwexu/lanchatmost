import asyncio
import json
import logging
import time
from typing import Optional, Dict, Any, Callable, List
import aiohttp

logger = logging.getLogger(__name__)


class LanChatClient:
    """Клиент для работы с LanChat API через HTTP Long Polling"""
    
    def __init__(self, token: str, ws_url: str, api_url: str = "https://msgpublic.langame.ru"):
        self.token = token
        self.ws_url = ws_url
        self.api_url = api_url
        self.subscribed_chats = set()
        self.message_handlers = []
        self._running = False
        self._stop_polling = False
        self.last_message_id = None  # Для отслеживания последнего сообщения
        self.poll_interval = 3  # Секунды между запросами
    
    def on_message(self, handler: Callable):
        """Регистрация обработчика сообщений"""
        self.message_handlers.append(handler)
        return handler
    
    async def subscribe(self, chat_id: str):
        """Подписка на чат (для совместимости)"""
        self.subscribed_chats.add(chat_id)
        logger.info(f"✅ Подписка на чат {chat_id} выполнена")
        return True
    
    async def listen(self):
        """Прослушивание новых сообщений через HTTP polling"""
        if not self.subscribed_chats:
            logger.error("Нет подписанных чатов")
            return
        
        self._running = True
        self._stop_polling = False
        
        # Сохраняем ID последнего обработанного сообщения для каждого чата
        last_message_ids = {}
        
        while self._running and not self._stop_polling:
            try:
                for chat_id in self.subscribed_chats:
                    if self._stop_polling:
                        break
                    
                    # Получаем новые сообщения
                    messages = await self.get_messages(chat_id, last_message_ids.get(chat_id))
                    
                    if messages:
                        # Обрабатываем сообщения в обратном порядке (старые -> новые)
                        for msg_data in reversed(messages):
                            msg_id = msg_data.get("id")
                            if msg_id:
                                # Обновляем последний ID
                                if not last_message_ids.get(chat_id) or msg_id > last_message_ids[chat_id]:
                                    last_message_ids[chat_id] = msg_id
                                
                                # Вызываем обработчики
                                for handler in self.message_handlers:
                                    try:
                                        if asyncio.iscoroutinefunction(handler):
                                            await handler(chat_id, msg_data)
                                        else:
                                            handler(chat_id, msg_data)
                                    except Exception as e:
                                        logger.error(f"Ошибка в обработчике: {e}")
                
                # Ждем перед следующим запросом
                await asyncio.sleep(self.poll_interval)
                
            except Exception as e:
                logger.error(f"Ошибка в polling: {e}")
                await asyncio.sleep(self.poll_interval * 2)
    
    async def get_messages(self, chat_id: str, after_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """Получение сообщений из чата через HTTP API"""
        # Используем эндпоинт для получения истории сообщений
        url = f"{self.api_url}/api/public/chats/{chat_id}/messages"
        headers = {"Authorization": f"Bearer {self.token}"}
        
        params = {
            "limit": 20  # Максимальное количество сообщений за раз
        }
        
        if after_id:
            # Если есть ID последнего сообщения, запрашиваем только новые
            # Примечание: может потребоваться другой параметр, если API поддерживает
            params["from"] = after_id
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, headers=headers, params=params) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        messages = data.get("messages", data.get("items", []))
                        return messages
                    else:
                        # Если эндпоинт не работает, пробуем альтернативный
                        return await self.get_messages_alternative(chat_id, after_id)
        except Exception as e:
            logger.error(f"Ошибка получения сообщений: {e}")
            return []
    
    async def get_messages_alternative(self, chat_id: str, after_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """Альтернативный метод получения сообщений"""
        # Пытаемся получить через WebSocket эмуляцию или другой эндпоинт
        # Используем метод, который возвращает последние сообщения
        url = f"{self.api_url}/api/public/chats/{chat_id}/messages/latest"
        headers = {"Authorization": f"Bearer {self.token}"}
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, headers=headers) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        return data.get("messages", data.get("items", []))
                    else:
                        logger.warning(f"Альтернативный метод не работает: {resp.status}")
                        return []
        except Exception as e:
            logger.error(f"Ошибка в альтернативном методе: {e}")
            return []
    
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
        """Остановка клиента"""
        self._running = False
        self._stop_polling = True
        logger.info("Клиент остановлен")