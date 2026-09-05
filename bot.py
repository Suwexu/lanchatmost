import asyncio
import logging
import os
import sys
import re
from datetime import datetime
from typing import Optional, List

from telegram import Bot, Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    CallbackQueryHandler,
    filters,
)

from lanchat_client import LanChatClient

# Настройка логирования
log_level = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, log_level, logging.INFO),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

# === ПЕРЕМЕННЫЕ ОКРУЖЕНИЯ ===
TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
LANCHAT_TOKEN = os.getenv("LANCHAT_API_TOKEN")
LANCHAT_WS_URL = os.getenv("LANCHAT_WS_URL", "wss://msgpublic.langame.ru/wsapi")
LANCHAT_API_URL = os.getenv("LANCHAT_API_URL", "https://msgpublic.langame.ru")

# Проверка обязательных переменных
required_vars = {
    "TELEGRAM_BOT_TOKEN": TELEGRAM_TOKEN,
    "TELEGRAM_CHAT_ID": TELEGRAM_CHAT_ID,
    "LANCHAT_API_TOKEN": LANCHAT_TOKEN,
}

missing_vars = [key for key, value in required_vars.items() if not value]
if missing_vars:
    logger.error(f"❌ Отсутствуют переменные: {', '.join(missing_vars)}")
    sys.exit(1)

if LANCHAT_TOKEN and not LANCHAT_TOKEN.startswith("lpat_"):
    logger.error(f"❌ Неправильный формат токена! Должен начинаться с 'lpat_'")
    sys.exit(1)

# Глобальные переменные
lanchat_client: Optional[LanChatClient] = None
telegram_bot: Optional[Bot] = None
processed_messages = set()
selected_chat_id: Optional[str] = None
selected_chat_title: Optional[str] = None
available_chats_cache: List[dict] = []
ws_connected = False


# ==================== ФУНКЦИИ ДЛЯ РАБОТЫ С LANCHAT ====================

async def send_to_lanchat(text: str, reply_to_id: Optional[str] = None):
    """Отправка сообщения в LanChat через HTTP POST"""
    if not text or not selected_chat_id:
        return None
    
    url = f"{LANCHAT_API_URL}/api/public/chats/{selected_chat_id}/messages"
    headers = {
        "Authorization": f"Bearer {LANCHAT_TOKEN}",
        "Content-Type": "application/json"
    }
    
    payload = {"text": text}
    if reply_to_id:
        payload["replyToMessageId"] = reply_to_id
    
    try:
        timeout = aiohttp.ClientTimeout(total=30)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.post(url, json=payload, headers=headers) as resp:
                if resp.status == 200:
                    result = await resp.json()
                    logger.info(f"✅ Сообщение отправлено в LanChat")
                    return result
                else:
                    error = await resp.text()
                    logger.error(f"❌ Ошибка: {resp.status} - {error}")
                    return None
    except Exception as e:
        logger.error(f"❌ Ошибка: {e}")
        return None


# ... остальные функции (send_media_to_lanchat, format_lanchat_message, 
# handle_lanchat_message, handle_telegram_message) остаются без изменений ...


# ==================== ПОИСК И ВЫБОР ЧАТА ====================

async def refresh_chats_cache() -> List[dict]:
    """Обновление кэша доступных чатов через WebSocket"""
    global available_chats_cache, ws_connected
    
    if not lanchat_client:
        return []
    
    try:
        # Получаем список чатов через WebSocket
        chats = await lanchat_client.get_available_chats()
        if chats:
            available_chats_cache = chats
            ws_connected = True
            logger.info(f"📋 Обновлен кэш чатов: {len(chats)} доступно")
            return chats
        else:
            ws_connected = False
            return []
    except Exception as e:
        logger.error(f"Ошибка обновления кэша чатов: {e}")
        ws_connected = False
        return []


async def auto_select_chat() -> Optional[str]:
    """Автоматический выбор чата при запуске"""
    global selected_chat_id, selected_chat_title
    
    chats = await refresh_chats_cache()
    
    if not chats:
        logger.warning("⚠️ Нет доступных чатов")
        return None
    
    if len(chats) == 1:
        chat = chats[0]
        selected_chat_id = chat.get("id")
        selected_chat_title = chat.get("title", "Без названия")
        logger.info(f"✅ Автоматически выбран единственный чат: {selected_chat_title}")
        return selected_chat_id
    
    first_chat = chats[0]
    selected_chat_id = first_chat.get("id")
    selected_chat_title = first_chat.get("title", "Без названия")
    logger.info(f"📋 Доступно {len(chats)} чатов")
    logger.info(f"✅ Выбран первый чат: {selected_chat_title}")
    logger.info("💡 Используйте /select_chat для выбора другого чата")
    
    return selected_chat_id


async def show_chat_selection(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показать интерактивный выбор чата"""
    if not lanchat_client:
        await update.message.reply_text("❌ Бот не подключен к LanChat")
        return
    
    await update.message.reply_text("🔄 Получение списка чатов через WebSocket...")
    
    chats = await refresh_chats_cache()
    
    if not chats:
        await update.message.reply_text(
            "❌ Нет доступных чатов\n\n"
            "Проверьте:\n"
            "1. Правильность токена LanChat\n"
            "2. Подключение к WebSocket\n"
            "3. Вы состоите хотя бы в одном чате\n\n"
            "Используйте /status для диагностики"
        )
        return
    
    # Строим клавиатуру
    keyboard = []
    for chat in chats[:20]:
        chat_id = chat.get("id")
        chat_title = chat.get("title", "Без названия")
        is_channel = chat.get("isChannel", False)
        icon = "📢" if is_channel else "💬"
        
        button_text = f"{icon} {chat_title[:30]}"
        if chat_id == selected_chat_id:
            button_text = "✅ " + button_text
        
        button = InlineKeyboardButton(
            button_text,
            callback_data=f"select_chat_{chat_id}"
        )
        keyboard.append([button])
    
    keyboard.append([
        InlineKeyboardButton("🔄 Обновить", callback_data="refresh_chats"),
        InlineKeyboardButton("❌ Отмена", callback_data="select_cancel")
    ])
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    current_chat = f"Текущий чат: <b>{selected_chat_title or 'Не выбран'}</b>" if selected_chat_id else "⚠️ Чат не выбран"
    
    await update.message.reply_text(
        f"📋 <b>Выберите чат для пересылки</b>\n\n"
        f"{current_chat}\n"
        f"Всего чатов: <b>{len(chats)}</b>\n"
        f"Подключение: {'✅ WebSocket' if ws_connected else '❌ Нет'}\n\n"
        f"Нажмите на название, чтобы подписаться:",
        parse_mode="HTML",
        reply_markup=reply_markup
    )


# ... остальные функции (handle_chat_selection, команды, main) остаются без изменений ...


# ==================== MAIN ====================

async def main():
    global lanchat_client, selected_chat_id, selected_chat_title, ws_connected
    
    logger.info("🚀 Запуск LanChat ↔ Telegram бота")
    logger.info("🔄 Режим: ДВУСТОРОННЯЯ СИНХРОНИЗАЦИЯ")
    logger.info(f"🌐 WebSocket URL: {LANCHAT_WS_URL}")
    logger.info(f"🔑 Токен: {LANCHAT_TOKEN[:10]}...")
    
    # Создаем клиент с WebSocket
    lanchat_client = LanChatClient(LANCHAT_TOKEN, LANCHAT_WS_URL, LANCHAT_API_URL)
    lanchat_client.on_message(handle_lanchat_message)
    
    # Подключаемся к WebSocket
    try:
        if await lanchat_client.connect():
            logger.info("✅ WebSocket подключен")
            
            # Авторизуемся
            await lanchat_client.authenticate()
            
            # Запускаем прослушивание
            asyncio.create_task(lanchat_client.listen())
            logger.info("📡 WebSocket прослушивание запущено")
            
            # Ждем получения списка чатов
            await asyncio.sleep(2)
            
            # Получаем чаты
            chats = await lanchat_client.get_available_chats()
            if chats:
                available_chats_cache = chats
                ws_connected = True
                logger.info(f"📋 Получено {len(chats)} чатов")
                
                # Автоматический выбор чата
                chat_id = await auto_select_chat()
                if chat_id:
                    await lanchat_client.subscribe(chat_id)
                    logger.info(f"✅ Подписка на чат {chat_id} выполнена")
            else:
                logger.warning("⚠️ Нет доступных чатов. Используйте /select_chat")
        else:
            logger.error("❌ Не удалось подключиться к WebSocket")
            logger.info("💡 Бот будет работать в режиме ожидания команд")
    except Exception as e:
        logger.error(f"❌ Ошибка настройки: {e}")
        import traceback
        traceback.print_exc()
    
    # Запускаем Telegram бота
    await start_telegram_bot()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("👋 Бот остановлен")
    except Exception as e:
        logger.error(f"❌ Критическая ошибка: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)