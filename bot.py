import asyncio
import logging
import os
import sys
import re
from datetime import datetime
from typing import Optional, List

import aiohttp
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

# СПИСОК АЛЬТЕРНАТИВНЫХ URL ДЛЯ API
API_URLS = [
    os.getenv("LANCHAT_API_URL", "https://msgpublic.langame.ru"),
    "https://msgtp.langame.ru",  # Альтернативный домен
    "https://api.langame.ru",    # Еще один возможный
]

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

# Глобальные переменные
lanchat_client: Optional[LanChatClient] = None
telegram_bot: Optional[Bot] = None
processed_messages = set()
selected_chat_id: Optional[str] = None
selected_chat_title: Optional[str] = None
available_chats_cache: List[dict] = []
active_api_url: Optional[str] = None


# ==================== ПРОВЕРКА ДОСТУПНОСТИ API ====================

async def test_api_connection(url: str) -> bool:
    """Проверка доступности API"""
    try:
        timeout = aiohttp.ClientTimeout(total=10)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            # Пробуем получить список чатов (быстрый тест)
            test_url = f"{url}/api/public/chats"
            headers = {"Authorization": f"Bearer {LANCHAT_TOKEN}"}
            
            async with session.get(test_url, headers=headers) as resp:
                if resp.status in [200, 401, 403]:
                    # 401/403 означает, что сервер доступен, но токен может быть неверным
                    logger.info(f"✅ API доступен: {url} (статус: {resp.status})")
                    return True
                return False
    except Exception as e:
        logger.warning(f"❌ API недоступен: {url} - {e}")
        return False


async def find_working_api() -> Optional[str]:
    """Поиск работающего API"""
    global active_api_url
    
    logger.info("🔍 Поиск доступного API...")
    
    # Сначала пробуем URL из переменных
    for url in API_URLS:
        logger.info(f"🔄 Проверка: {url}")
        if await test_api_connection(url):
            active_api_url = url
            logger.info(f"✅ Выбран API: {url}")
            return url
        
        await asyncio.sleep(1)  # Небольшая задержка между попытками
    
    logger.error("❌ Ни один API не доступен")
    return None


# ==================== ПОИСК ЧАТОВ ====================

async def refresh_chats_cache() -> List[dict]:
    """Обновление кэша доступных чатов"""
    global available_chats_cache, active_api_url
    
    if not lanchat_client:
        return []
    
    if not active_api_url:
        # Пытаемся найти работающий API
        api_url = await find_working_api()
        if not api_url:
            logger.error("❌ Не удалось найти работающий API")
            return []
        # Обновляем URL в клиенте
        lanchat_client.api_url = api_url
    
    try:
        chats = await lanchat_client.get_available_chats()
        available_chats_cache = chats
        logger.info(f"📋 Обновлен кэш чатов: {len(chats)} доступно")
        return chats
    except Exception as e:
        logger.error(f"Ошибка обновления кэша чатов: {e}")
        # Если ошибка, пробуем следующий API
        active_api_url = None
        return []


async def auto_select_chat() -> Optional[str]:
    """Автоматический выбор чата при запуске"""
    global selected_chat_id, selected_chat_title
    
    chats = await refresh_chats_cache()
    
    if not chats:
        logger.warning("⚠️ Нет доступных чатов")
        return None
    
    # Если есть только один чат - выбираем его
    if len(chats) == 1:
        chat = chats[0]
        selected_chat_id = chat.get("id")
        selected_chat_title = chat.get("title", "Без названия")
        logger.info(f"✅ Автоматически выбран единственный чат: {selected_chat_title}")
        return selected_chat_id
    
    # Если несколько чатов - выбираем первый
    first_chat = chats[0]
    selected_chat_id = first_chat.get("id")
    selected_chat_title = first_chat.get("title", "Без названия")
    logger.info(f"📋 Доступно {len(chats)} чатов")
    logger.info(f"✅ Выбран первый чат: {selected_chat_title}")
    logger.info("💡 Используйте /select_chat для выбора другого чата")
    
    return selected_chat_id


# ==================== ВЫБОР ЧАТА ====================

async def show_chat_selection(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показать интерактивный выбор чата"""
    if not lanchat_client:
        await update.message.reply_text("❌ Бот не подключен к LanChat")
        return
    
    await update.message.reply_text("🔄 Получение списка чатов...")
    
    chats = await refresh_chats_cache()
    
    if not chats:
        await update.message.reply_text(
            "❌ Нет доступных чатов\n\n"
            "Возможные причины:\n"
            "1. Неправильный токен LanChat\n"
            "2. Вы не состоите ни в одном чате\n"
            "3. API LanChat недоступен\n"
            "4. Проблемы с сетью (попробуйте позже)\n\n"
            "Проверьте токен и попробуйте снова."
        )
        return
    
    # Строим клавиатуру
    keyboard = []
    for chat in chats[:20]:
        chat_id = chat.get("id")
        chat_title = chat.get("title", "Без названия")
        is_channel = chat.get("isChannel", False)
        icon = "📢" if is_channel else "💬"
        
        participants = chat.get("participants", [])
        members_count = len(participants) if participants else "?"
        
        button_text = f"{icon} {chat_title[:25]} ({members_count} уч.)"
        if len(button_text) > 40:
            button_text = button_text[:37] + "..."
        
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
        f"API: {active_api_url or 'Неизвестно'}\n\n"
        f"Нажмите на название, чтобы подписаться:",
        parse_mode="HTML",
        reply_markup=reply_markup
    )


async def handle_chat_selection(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик выбора чата"""
    query = update.callback_query
    await query.answer()
    
    data = query.data
    global selected_chat_id, selected_chat_title
    
    if data == "select_cancel":
        await query.edit_message_text("❌ Выбор отменен")
        return
    
    if data == "refresh_chats":
        await query.edit_message_text("🔄 Обновление списка чатов...")
        # Сбрасываем активный API, чтобы попробовать другой
        global active_api_url
        active_api_url = None
        chats = await refresh_chats_cache()
        
        if not chats:
            await query.edit_message_text("❌ Нет доступных чатов")
            return
        
        # Перестраиваем клавиатуру
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
        
        await query.edit_message_text(
            f"📋 <b>Выберите чат для пересылки</b>\n\n"
            f"{current_chat}\n"
            f"Всего чатов: <b>{len(chats)}</b>\n"
            f"API: {active_api_url or 'Неизвестно'}",
            parse_mode="HTML",
            reply_markup=reply_markup
        )
        return
    
    if data.startswith("select_chat_"):
        chat_id = data.replace("select_chat_", "")
        
        # Находим чат в кэше
        chat_info = None
        for chat in available_chats_cache:
            if chat.get("id") == chat_id:
                chat_info = chat
                break
        
        if not chat_info:
            await query.edit_message_text("❌ Чат не найден. Обновите список.")
            return
        
        chat_title = chat_info.get("title", "Без названия")
        is_channel = chat_info.get("isChannel", False)
        chat_type = "канал" if is_channel else "чат"
        
        selected_chat_id = chat_id
        selected_chat_title = chat_title
        
        await lanchat_client.subscribe(chat_id)
        
        # Перезапускаем polling
        if lanchat_client._running:
            lanchat_client._stop_polling = True
            await asyncio.sleep(1)
            lanchat_client._stop_polling = False
            asyncio.create_task(lanchat_client.listen())
        
        await query.edit_message_text(
            f"✅ <b>Чат выбран!</b>\n\n"
            f"📌 Название: <b>{chat_title}</b>\n"
            f"📋 Тип: {chat_type}\n"
            f"🆔 ID: <code>{chat_id}</code>\n\n"
            f"📊 Режим: HTTP Polling\n"
            f"🌐 API: {active_api_url}\n\n"
            f"Теперь все сообщения из этого чата будут пересылаться в Telegram.",
            parse_mode="HTML"
        )
        
        logger.info(f"✅ Выбран чат: {chat_title} ({chat_id})")


# ==================== ОСТАЛЬНЫЕ ФУНКЦИИ (без изменений) ====================
# ... (оставляем те же функции: send_to_lanchat, send_media_to_lanchat,
# format_lanchat_message, handle_lanchat_message, handle_telegram_message)


# ==================== TELEGRAM БОТ ====================

async def start_telegram_bot():
    """Запуск Telegram бота"""
    global telegram_bot
    
    application = Application.builder().token(TELEGRAM_TOKEN).build()
    telegram_bot = application.bot
    
    # Команды
    application.add_handler(CommandHandler("start", start_command))
    application.add_handler(CommandHandler("status", status_command))
    application.add_handler(CommandHandler("ping", ping_command))
    application.add_handler(CommandHandler("select_chat", select_chat_command))
    application.add_handler(CommandHandler("help", help_command))
    
    # Обработчики callback query
    application.add_handler(CallbackQueryHandler(handle_chat_selection, pattern="^select_"))
    
    # Обработчики сообщений (Telegram → LanChat)
    application.add_handler(MessageHandler(
        filters.TEXT & ~filters.COMMAND & filters.Chat(chat_id=int(TELEGRAM_CHAT_ID)),
        handle_telegram_message
    ))
    
    application.add_handler(MessageHandler(
        filters.ATTACHMENT & filters.Chat(chat_id=int(TELEGRAM_CHAT_ID)),
        send_media_to_lanchat
    ))
    
    # Запуск
    await application.initialize()
    await application.start()
    await application.updater.start_polling()
    
    while True:
        await asyncio.sleep(1)


# ===== КОМАНДЫ =====

async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if str(update.effective_chat.id) != TELEGRAM_CHAT_ID:
        await update.message.reply_text("❌ Доступ запрещен.")
        return
    
    chat_info = f"<b>{selected_chat_title}</b> (<code>{selected_chat_id}</code>)" if selected_chat_id else "❌ Не выбран"
    api_status = active_api_url or "⏳ Поиск..."
    
    await update.message.reply_text(
        f"🤖 <b>LanChat ↔ Telegram Бот</b>\n\n"
        f"✅ Статус: <b>Активен</b>\n"
        f"📡 Текущий чат: {chat_info}\n"
        f"🌐 API: {api_status}\n"
        f"🔄 <b>Двусторонняя синхронизация</b>\n"
        f"📊 Режим: <b>HTTP Polling</b>\n\n"
        "💡 <b>Первые шаги:</b>\n"
        "1. Используйте /select_chat для выбора чата\n"
        "2. Бот покажет все доступные чаты\n"
        "3. Выберите нужный чат и начнется пересылка\n\n"
        "📋 Команды:\n"
        "/status - Статус бота\n"
        "/ping - Проверка соединения\n"
        "/select_chat - Выбрать чат\n"
        "/help - Помощь",
        parse_mode="HTML"
    )


async def status_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if str(update.effective_chat.id) != TELEGRAM_CHAT_ID:
        await update.message.reply_text("❌ Доступ запрещен.")
        return
    
    status = "✅ Активен" if lanchat_client and lanchat_client._running else "❌ Остановлен"
    
    chat_info = "❌ Не выбран"
    if selected_chat_id:
        chat_info = f"<b>{selected_chat_title}</b> (<code>{selected_chat_id}</code>)"
    
    await update.message.reply_text(
        f"📊 <b>Статус бота</b>\n\n"
        f"LanChat: {status}\n"
        f"Чат: {chat_info}\n"
        f"API: {active_api_url or 'Не найден'}\n"
        f"Обработано сообщений: <b>{len(processed_messages)}</b>\n"
        f"Доступно чатов: <b>{len(available_chats_cache)}</b>\n"
        f"Режим: <b>HTTP Polling</b>",
        parse_mode="HTML"
    )


async def ping_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if str(update.effective_chat.id) != TELEGRAM_CHAT_ID:
        await update.message.reply_text("❌ Доступ запрещен.")
        return
    await update.message.reply_text("🏓 Pong! Бот работает")


async def select_chat_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if str(update.effective_chat.id) != TELEGRAM_CHAT_ID:
        await update.message.reply_text("❌ Доступ запрещен.")
        return
    await show_chat_selection(update, context)


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if str(update.effective_chat.id) != TELEGRAM_CHAT_ID:
        await update.message.reply_text("❌ Доступ запрещен.")
        return
    
    await update.message.reply_text(
        "📖 <b>Помощь</b>\n\n"
        "🔄 <b>Двусторонняя синхронизация</b>\n\n"
        "1️⃣ <b>LanChat → Telegram</b>\n"
        "   - Сообщения проверяются каждые 3 секунды\n"
        "   - Поддерживаются: текст, стикеры, вложения, реакции\n\n"
        "2️⃣ <b>Telegram → LanChat</b>\n"
        "   - Отправьте текст → появится в LanChat\n"
        "   - Ответьте на сообщение бота → ответ в LanChat\n"
        "   - Отправьте фото/документ → отправится в LanChat\n\n"
        "💡 <b>Как начать:</b>\n"
        "   1. Используйте /select_chat\n"
        "   2. Выберите чат из списка\n"
        "   3. Бот начнет пересылку\n\n"
        "⚙️ <b>Если чаты не отображаются:</b>\n"
        "   - Проверьте токен LanChat\n"
        "   - Попробуйте /select_chat через 1-2 минуты\n"
        "   - Убедитесь, что вы состоите в чате\n\n"
        "Команды:\n"
        "/start - Старт\n"
        "/status - Статус\n"
        "/ping - Проверка\n"
        "/select_chat - Выбрать чат\n"
        "/help - Помощь",
        parse_mode="HTML"
    )


# ==================== MAIN ====================

async def main():
    global lanchat_client, selected_chat_id, selected_chat_title, active_api_url
    
    logger.info("🚀 Запуск LanChat ↔ Telegram бота")
    logger.info("🔄 Режим: ДВУСТОРОННЯЯ СИНХРОНИЗАЦИЯ")
    logger.info("📡 Режим работы: HTTP Polling")
    logger.info("🔑 Авторизация по токену LanChat")
    
    # Ищем работающий API
    api_url = await find_working_api()
    if not api_url:
        logger.error("❌ Не найден доступный API. Бот будет работать только с командой /select_chat")
        # Но продолжаем, чтобы бот хотя бы отвечал на команды
    
    # Создаем клиент
    lanchat_client = LanChatClient(LANCHAT_TOKEN, api_url or API_URLS[0])
    lanchat_client.on_message(handle_lanchat_message)
    lanchat_client.poll_interval = 5  # Увеличиваем интервал для экономии
    
    # Автоматический поиск чатов (если API доступен)
    if api_url:
        try:
            logger.info("🔍 Поиск доступных чатов...")
            chat_id = await auto_select_chat()
            
            if chat_id:
                await lanchat_client.subscribe(chat_id)
                logger.info(f"✅ Подписка на чат {chat_id} выполнена")
                asyncio.create_task(lanchat_client.listen())
                logger.info("✅ HTTP Polling запущен")
            else:
                logger.warning("⚠️ Чаты не найдены. Используйте /select_chat в Telegram")
        except Exception as e:
            logger.error(f"❌ Ошибка настройки: {e}")
    else:
        logger.warning("⚠️ API недоступен. Используйте /select_chat для повторной попытки")
    
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