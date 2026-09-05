import asyncio
import json
import logging
import msgpack
import websockets
from typing import Optional, Dict, Any, Callable
from tenacity import retry, stop_after_attempt, wait_exponential

logger = logging.getLogger(__name__)


class LanChatClient:
    """Клиент для работы с LanChat API через WebSocket"""
    
    def __init__(self, token: str, ws_url: str):
        self.token = token
        self.ws_url = ws_url
        self.websocket: Optional[websockets.WebSocketClientProtocol] = None
        self.subscribed_chats = set()
        self.message_handlers = []
        self._running = False
        self._reconnect_task = None
        
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
                
                # Отправляем auth кадр для подтверждения
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
            logger.error(f"Не удалось подключиться после всех попыток: {e}")
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
            logger.error(f"Ошибка подписки на чат {chat_id}: {e}")
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
                    # Декодируем MessagePack
                    message = msgpack.unpackb(raw_message, raw=False)
                    logger.debug(f"Получено сообщение: {json.dumps(message, ensure_ascii=False, default=str)}")
                    
                    # Проверяем тип сообщения
                    msg_type = message.get("t")
                    
                    if msg_type == "authed":
                        logger.info("✅ Авторизация в LanChat успешна")
                        # После авторизации подписываемся на чаты
                        for chat_id in self.subscribed_chats:
                            await self.subscribe(chat_id)
                    
                    elif msg_type == "chats_available":
                        logger.info(f"Доступные чаты: {len(message.get('chats', []))}")
                    
                    elif msg_type == "subscribed":
                        chat_id = message.get("chatId")
                        logger.info(f"✅ Подписка на чат {chat_id} подтверждена")
                    
                    elif msg_type == "message_new":
                        # Новое сообщение - обрабатываем
                        chat_id = message.get("chatId")
                        msg_data = message.get("message", {})
                        logger.info(f"📨 Новое сообщение в чате {chat_id} от {msg_data.get('user', {}).get('name', 'Unknown')}")
                        
                        # Вызываем все зарегистрированные обработчики
                        for handler in self.message_handlers:
                            try:
                                if asyncio.iscoroutinefunction(handler):
                                    await handler(chat_id, msg_data)
                                else:
                                    handler(chat_id, msg_data)
                            except Exception as e:
                                logger.error(f"Ошибка в обработчике сообщения: {e}")
                    
                    elif msg_type == "message_update":
                        # Обновление сообщения (опционально)
                        logger.debug(f"Обновление сообщения: {message.get('message', {}).get('id')}")
                    
                    elif msg_type == "error":
                        error_code = message.get("code")
                        error_msg = message.get("message", "Unknown error")
                        logger.error(f"Ошибка LanChat: {error_code} - {error_msg}")
                        
                        if error_code in ["unauthorized", "auth_failed"]:
                            logger.critical("❌ Ошибка авторизации! Проверьте токен")
                            break
                    
                    elif msg_type == "pong":
                        logger.debug("Pong получен")
                    
                    else:
                        logger.debug(f"Получен кадр типа {msg_type}")
                        
                except msgpack.exceptions.UnpackException as e:
                    logger.error(f"Ошибка распаковки MessagePack: {e}")
                except Exception as e:
                    logger.error(f"Ошибка обработки сообщения: {e}")
                    
        except websockets.exceptions.ConnectionClosed as e:
            logger.warning(f"WebSocket соединение закрыто: {e}")
            self._running = False
            # Попытка переподключения
            await self._reconnect()
        except Exception as e:
            logger.error(f"Ошибка в listen: {e}")
            self._running = False
    
    async def _reconnect(self):
        """Переподключение при обрыве соединения"""
        if self._running:
            logger.info("Попытка переподключения через 5 секунд...")
            await asyncio.sleep(5)
            
            if await self.connect():
                # Заново подписываемся на все чаты
                for chat_id in self.subscribed_chats:
                    await self.subscribe(chat_id)
                
                # Запускаем прослушивание заново
                asyncio.create_task(self.listen())
            else:
                logger.error("Не удалось переподключиться")
    
    async def close(self):
        """Закрытие соединения"""
        self._running = False
        if self.websocket:
            await self.websocket.close()
            logger.info("WebSocket соединение закрыто")