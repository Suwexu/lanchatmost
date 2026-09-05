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

# Проверка формата токена LanChat
if LANCHAT_TOKEN and not LANCHAT_TOKEN.startswith("lpat_"):
    logger.error(f"❌ Неправильный формат токена LanChat! Должен начинаться с 'lpat_'")
    logger.error(f"Текущий токен: {LANCHAT_TOKEN[:10]}...")
    sys.exit(1)

# Глобальные переменные
lanchat_client: Optional[LanChatClient] = None
telegram_bot: Optional[Bot] = None
processed_messages = set()
selected_chat_id: Optional[str] = None
selected_chat_title: Optional[str] = None
available_chats_cache: List[dict] = []
api_available = False


# ==================== ПРОВЕРКА ТОКЕНА ====================

async def test_lanchat_token() -> tuple[bool, List[dict]]:
    """Проверка токена LanChat и получение списка чатов"""
    url = f"{LANCHAT_API_URL}/api/public/chats"
    headers = {"Authorization": f"Bearer {LANCHAT_TOKEN}"}
    
    logger.info(f"🔍 Проверка токена LanChat...")
    logger.info(f"🌐 API URL: {LANCHAT_API_URL}")
    logger.info(f"🔑 Токен: {LANCHAT_TOKEN[:10]}... (первые 10 символов)")
    
    try:
        timeout = aiohttp.ClientTimeout(total=15, connect=10)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(url, headers=headers) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    chats = data.get("chats", data.get("data", data.get("items", [])))
                    logger.info(f"✅ Токен правильный! Найдено {len(chats)} чатов")
                    
                    if chats:
                        for i, chat in enumerate(chats[:5], 1):
                            logger.info(f"  {i}. {chat.get('title', 'Без названия')} (ID: {chat.get('id')})")
                        if len(chats) > 5:
                            logger.info(f"  ... и еще {len(chats) - 5} чатов")
                    
                    return True, chats
                elif resp.status == 401:
                    error_text = await resp.text()
                    logger.error(f"❌ Токен НЕДЕЙСТВИТЕЛЕН! (401 Unauthorized)")
                    logger.error(f"📝 Ответ сервера: {error_text}")
                    logger.error(f"💡 Проверьте:")
                    logger.error(f"   • Токен начинается с 'lpat_'")
                    logger.error(f"   • Токен скопирован полностью")
                    logger.error(f"   • Токен не истек")
                    return False, []
                elif resp.status == 403:
                    logger.error(f"❌ Доступ запрещен (403 Forbidden)")
                    logger.error(f"💡 Возможно, у токена нет прав для чтения чатов")
                    return False, []
                else:
                    error_text = await resp.text()
                    logger.error(f"❌ Ошибка {resp.status}: {error_text}")
                    return False, []
    except asyncio.TimeoutError:
        logger.error(f"❌ Таймаут подключения к {LANCHAT_API_URL}")
        logger.error(f"💡 Проверьте:")
        logger.error(f"   • Доступность API LanChat")
        logger.error(f"   • Настройки сети в Railway")
        return False, []
    except aiohttp.ClientConnectorError as e:
        logger.error(f"❌ Ошибка соединения: {e}")
        logger.error(f"💡 Проверьте:")
        logger.error(f"   • Правильность URL: {LANCHAT_API_URL}")
        logger.error(f"   • Доступность интернета в Railway")
        return False, []
    except Exception as e:
        logger.error(f"❌ Неизвестная ошибка: {e}")
        import traceback
        traceback.print_exc()
        return False, []


# ==================== ОСТАЛЬНЫЕ ФУНКЦИИ ====================

async def refresh_chats_cache() -> List[dict]:
    """Обновление кэша доступных чатов"""
    global available_chats_cache, api_available
    
    if not lanchat_client:
        return []
    
    try:
        chats = await lanchat_client.get_available_chats()
        if chats is not None and len(chats) > 0:
            available_chats_cache = chats
            api_available = True
            logger.info(f"📋 Обновлен кэш чатов: {len(chats)} доступно")
            return chats
        else:
            api_available = False
            return []
    except Exception as e:
        logger.error(f"Ошибка обновления кэша чатов: {e}")
        api_available = False
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
    
    await update.message.reply_text("🔄 Получение списка чатов...")
    
    # Проверяем токен заново
    success, chats = await test_lanchat_token()
    
    if not success or not chats:
        await update.message.reply_text(
            "❌ <b>Ошибка подключения к LanChat</b>\n\n"
            "Проверьте:\n"
            "1. Токен должен начинаться с <code>lpat_</code>\n"
            "2. Токен должен быть активным\n"
            "3. Вы должны состоять хотя бы в одном чате\n\n"
            "Статус: <code>/status</code>\n"
            "Помощь: <code>/help</code>",
            parse_mode="HTML"
        )
        return
    
    available_chats_cache = chats
    api_available = True
    
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
        f"Всего чатов: <b>{len(chats)}</b>\n\n"
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
        success, chats = await test_lanchat_token()
        
        if not success or not chats:
            await query.edit_message_text("❌ Не удалось получить список чатов. Проверьте токен.")
            return
        
        available_chats_cache = chats
        api_available = True
        
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
            f"Всего чатов: <b>{len(chats)}</b>",
            parse_mode="HTML",
            reply_markup=reply_markup
        )
        return
    
    if data.startswith("select_chat_"):
        chat_id = data.replace("select_chat_", "")
        
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
            f"📊 Режим: HTTP Polling\n\n"
            f"Теперь все сообщения из этого чата будут пересылаться в Telegram.",
            parse_mode="HTML"
        )
        
        logger.info(f"✅ Выбран чат: {chat_title} ({chat_id})")


# ==================== ОТПРАВКА В LANCHAT ====================

async def send_to_lanchat(text: str, reply_to_id: Optional[str] = None):
    """Отправка сообщения в LanChat"""
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


async def send_media_to_lanchat(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Отправка медиа в LanChat"""
    if not selected_chat_id:
        await update.message.reply_text("❌ Сначала выберите чат через /select_chat")
        return
    
    message = update.message
    url = f"{LANCHAT_API_URL}/api/public/chats/{selected_chat_id}/messages"
    headers = {"Authorization": f"Bearer {LANCHAT_TOKEN}"}
    
    file = None
    file_name = "file"
    caption = message.caption or ""
    
    if message.photo:
        photo = message.photo[-1]
        file = await photo.get_file()
        file_name = f"photo_{photo.file_id}.jpg"
    elif message.document:
        file = await message.document.get_file()
        file_name = message.document.file_name or "document"
    elif message.video:
        file = await message.video.get_file()
        file_name = f"video_{message.video.file_id}.mp4"
    elif message.audio:
        file = await message.audio.get_file()
        file_name = f"audio_{message.audio.file_id}.mp3"
    else:
        await message.reply_text("❌ Неподдерживаемый тип файла")
        return
    
    try:
        file_data = await file.download_as_bytearray()
        
        data = aiohttp.FormData()
        if caption:
            data.add_field("text", caption)
        data.add_field("file", file_data, filename=file_name)
        
        timeout = aiohttp.ClientTimeout(total=60)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.post(url, data=data, headers=headers) as resp:
                if resp.status == 200:
                    result = await resp.json()
                    await message.reply_text(
                        f"✅ Медиа отправлено в LanChat\n<code>ID: {result.get('messageId')}</code>",
                        parse_mode="HTML"
                    )
                else:
                    await message.reply_text(f"❌ Ошибка: {resp.status}")
    except Exception as e:
        logger.error(f"Ошибка: {e}")
        await message.reply_text(f"❌ Ошибка: {str(e)}")


async def format_lanchat_message(msg_data: dict) -> str:
    """Форматирование сообщения из LanChat"""
    user = msg_data.get("user", {})
    username = user.get("name", "Unknown")
    user_login = user.get("login", "")
    is_verified = user.get("isVerified", False)
    
    user_display = username
    if user_login:
        user_display = f"{username} (@{user_login})"
    if is_verified:
        user_display = f"✅ {user_display}"
    
    text = msg_data.get("text", "")
    created_at = msg_data.get("createdAtIso", "")
    try:
        dt = datetime.fromisoformat(created_at.replace('Z', '+00:00'))
        time_str = dt.strftime("%H:%M:%S")
    except:
        time_str = created_at
    
    sticker = msg_data.get("sticker")
    sticker_text = f"\n[Стикер] {sticker.get('emoji', '')}" if sticker else ""
    
    attachments = msg_data.get("attachments", [])
    attachments_text = ""
    if attachments:
        attachments_text = "\n\n📎 Вложения:"
        for i, att in enumerate(attachments, 1):
            att_name = att.get("name", f"file_{i}")
            att_id = att.get("id", "")
            attachments_text += f"\n{i}. {att_name}"
            if att_id:
                attachments_text += f"\n   ID: {att_id}"
    
    reactions = msg_data.get("reactions", [])
    reactions_text = ""
    if reactions:
        reactions_list = [f"{r.get('emoji', '')} {r.get('count', 0)}" for r in reactions if r.get('count', 0) > 0]
        if reactions_list:
            reactions_text = f"\n\n📊 Реакции: {' '.join(reactions_list)}"
    
    message_parts = [
        f"💬 <b>Новое сообщение</b>",
        f"👤 {user_display}",
        f"🕐 {time_str}",
    ]
    
    if text:
        message_parts.append(f"\n📝 {text}")
    if sticker_text:
        message_parts.append(sticker_text)
    if attachments_text:
        message_parts.append(attachments_text)
    if reactions_text:
        message_parts.append(reactions_text)
    
    message_parts.append(f"\n<code>ID: {msg_data.get('id', '')}</code>")
    
    return "\n".join(message_parts)


async def handle_lanchat_message(chat_id: str, msg_data: dict):
    """Обработчик сообщений из LanChat"""
    try:
        if chat_id != selected_chat_id:
            return
        
        msg_id = msg_data.get("id")
        if msg_id in processed_messages:
            return
        
        processed_messages.add(msg_id)
        if len(processed_messages) > 10000:
            processed_messages.clear()
        
        formatted_msg = await format_lanchat_message(msg_data)
        await telegram_bot.send_message(
            chat_id=TELEGRAM_CHAT_ID,
            text=formatted_msg,
            parse_mode="HTML",
            disable_web_page_preview=True
        )
        logger.debug(f"Сообщение отправлено в Telegram: {msg_id}")
        
    except Exception as e:
        logger.error(f"Ошибка: {e}")


async def handle_telegram_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик сообщений из Telegram"""
    if str(update.effective_chat.id) != TELEGRAM_CHAT_ID:
        await update.message.reply_text("❌ Доступ запрещен.")
        return
    
    if not selected_chat_id:
        await update.message.reply_text("❌ Чат не выбран. Используйте /select_chat")
        return
    
    message = update.message
    text = message.text
    
    if text and text.startswith('/'):
        return
    
    if text:
        reply_to_id = None
        if message.reply_to_message:
            reply_text = message.reply_to_message.text or ""
            if "ID:" in reply_text:
                match = re.search(r"ID:\s*([a-f0-9\-]+)", reply_text)
                if match:
                    reply_to_id = match.group(1)
        
        result = await send_to_lanchat(text, reply_to_id)
        if result:
            await update.message.reply_text(
                f"✅ Отправлено в LanChat\n<code>ID: {result.get('messageId')}</code>",
                parse_mode="HTML"
            )
        else:
            await update.message.reply_text("❌ Ошибка отправки в LanChat")


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
    
    # Обработчики сообщений
    application.add_handler(MessageHandler(
        filters.TEXT & ~filters.COMMAND & filters.Chat(chat_id=int(TELEGRAM_CHAT_ID)),
        handle_telegram_message
    ))
    
    application.add_handler(MessageHandler(
        filters.ATTACHMENT & filters.Chat(chat_id=int(TELEGRAM_CHAT_ID)),
        send_media_to_lanchat
    ))
    
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
    api_status = "✅ Доступен" if api_available else "❌ Недоступен"
    
    await update.message.reply_text(
        f"🤖 <b>LanChat ↔ Telegram Бот</b>\n\n"
        f"✅ Статус: <b>Активен</b>\n"
        f"📡 Текущий чат: {chat_info}\n"
        f"🌐 API: {api_status}\n"
        f"🔑 Токен: {LANCHAT_TOKEN[:10]}... (первые 10 символов)\n"
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
    
    api_status = "✅ Доступен" if api_available else "❌ Недоступен"
    
    await update.message.reply_text(
        f"📊 <b>Статус бота</b>\n\n"
        f"LanChat: {status}\n"
        f"Чат: {chat_info}\n"
        f"API: {api_status}\n"
        f"Токен: {LANCHAT_TOKEN[:10]}... (первые 10 символов)\n"
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
        "   - Сообщения проверяются каждые 5 секунд\n"
        "   - Поддерживаются: текст, стикеры, вложения, реакции\n\n"
        "2️⃣ <b>Telegram → LanChat</b>\n"
        "   - Отправьте текст → появится в LanChat\n"
        "   - Ответьте на сообщение бота → ответ в LanChat\n"
        "   - Отправьте фото/документ → отправится в LanChat\n\n"
        "💡 <b>Как начать:</b>\n"
        "   1. Используйте /select_chat\n"
        "   2. Выберите чат из списка\n"
        "   3. Бот начнет пересылку\n\n"
        "🔑 <b>Проверка токена:</b>\n"
        "   - Токен должен начинаться с <code>lpat_</code>\n"
        "   - Проверьте в /status первые 10 символов\n"
        "   - Если токен не работает, сгенерируйте новый в LanChat\n\n"
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
    global lanchat_client, selected_chat_id, selected_chat_title, api_available
    
    logger.info("🚀 Запуск LanChat ↔ Telegram бота")
    logger.info("🔄 Режим: ДВУСТОРОННЯЯ СИНХРОНИЗАЦИЯ")
    logger.info("📡 Режим работы: HTTP Polling")
    logger.info(f"🌐 API URL: {LANCHAT_API_URL}")
    logger.info(f"🔑 Токен: {LANCHAT_TOKEN[:10]}... (первые 10 символов)")
    
    # Проверяем токен
    success, chats = await test_lanchat_token()
    
    if success and chats:
        api_available = True
        available_chats_cache = chats
        logger.info(f"✅ Авторизация успешна! Найдено {len(chats)} чатов")
        
        # Создаем клиент
        lanchat_client = LanChatClient(LANCHAT_TOKEN, LANCHAT_API_URL)
        lanchat_client.on_message(handle_lanchat_message)
        lanchat_client.poll_interval = 5
        
        # Автоматический выбор чата
        chat_id = await auto_select_chat()
        if chat_id:
            await lanchat_client.subscribe(chat_id)
            logger.info(f"✅ Подписка на чат {chat_id} выполнена")
            asyncio.create_task(lanchat_client.listen())
            logger.info("✅ HTTP Polling запущен")
    else:
        api_available = False
        logger.error("❌ Авторизация не удалась. Проверьте токен LanChat!")
        logger.info("💡 Бот будет работать в режиме ожидания команд")
        
        # Создаем клиент все равно, чтобы команды работали
        lanchat_client = LanChatClient(LANCHAT_TOKEN, LANCHAT_API_URL)
        lanchat_client.on_message(handle_lanchat_message)
    
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