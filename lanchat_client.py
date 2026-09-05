import asyncio
import json
import logging
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
        """Подключение к WebSocket с увеличенным таймаутом"""
        uri = f"{self.ws_url}?token={self.token}"
        
        @retry(
            stop=stop_after_attempt(3),  # Уменьшил количество попыток
            wait=wait_exponential(multiplier=2, min=4, max=60)  # Увеличил время ожидания
        )
        async def connect_with_retry():
            try:
                logger.info(f"Подключение к LanChat WebSocket: {self.ws_url}")
                # Увеличил таймауты
                self.websocket = await websockets.connect(
                    uri,
                    ping_interval=20,
                    ping_timeout=60,  # Увеличил с 30 до 60
                    close_timeout=30,
                    max_size=10 * 1024 * 1024,  # 10MB
                    open_timeout=60  # Добавил таймаут на открытие соединения
                )
                logger.info("WebSocket подключен успешно")
                
                # Отправляем auth кадр в формате JSON
                auth_frame = json.dumps({"t": "auth", "token": self.token})
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
    
    # ... остальной код без изменений ...