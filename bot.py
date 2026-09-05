import asyncio
import logging
import os
import sys
import re
from datetime import datetime
from typing import Optional

import aiohttp
from dotenv import load_dotenv
from telegram import Bot, Update
from telegram.ext import (
    Application,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

from lanchat_client import LanChatClient

# Загрузка переменных окружения
load_dotenv()

# Настройка логирования
log_level = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, log_level, logging.INFO),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# Конфигурация
TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
LANCHAT_TOKEN = os.getenv("LANCHAT_API_TOKEN")
LANCHAT_CHAT_ID = os.getenv("LANCHAT_CHAT_ID")
LANCHAT_WS_URL = os.getenv("LANCHAT_WS_URL", "wss://msgpublic.langame.ru/wsapi")
LANCHAT_API_URL = os.getenv("LANCHAT_API_URL", "https://msgpublic.langame.ru")

# Проверка переменных
required_vars = {
    "TELEGRAM_BOT_TOKEN": TELEGRAM_TOKEN,
    "TELEGRAM_CHAT_ID": TELEGRAM_CHAT_ID,
    "LANCHAT_API_TOKEN": LANCHAT_TOKEN,
    "LANCHAT_CHAT_ID": LANCHAT_CHAT_ID,
}

missing_vars = [key for key, value in required_vars.items() if not value]
if missing_vars:
    logger.error(f"❌ Отсутствуют переменные: {', '.join(missing_vars)}")
    sys.exit(1)

# Глобальные переменные
lanchat_client: Optional[LanChatClient] = None
telegram_bot: Optional[Bot] = None
processed_messages = set()


# ==================== ОТПРАВКА В LANCHAT ====================

async def send_to_lanchat(text: str, reply_to_id: Optional[str] = None):
    """Отправка сообщения в LanChat"""
    if not text:
        return None
    
    url = f"{LANCHAT_API_URL}/api/public/chats/{LANCHAT_CHAT_ID}/messages"
    headers = {
        "Authorization": f"Bearer {LANCHAT_TOKEN}",
        "Content-Type": "application/json"
    }
    
    payload = {"text": text}
    if reply_to_id:
        payload["replyToMessageId"] = reply_to_id
    
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=payload, headers=headers) as resp:
                if resp.status == 200:
                    result = await resp.json()
                    logger.info(f"✅ Сообщение отправлено в LanChat: {result.get('messageId')}")
                    return result
                else:
                    error = await resp.text()
                    logger.error(f"❌ Ошибка: {resp.status} - {error}")
                    return None
    except Exception as e:
        logger.error(f"❌ Ошибка: {e}")
        return None


async def send_media_to_lanchat(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Отправка медиа из Telegram в LanChat"""
    message = update.message
    url = f"{LANCHAT_API_URL}/api/public/chats/{LANCHAT_CHAT_ID}/messages"
    headers = {"Authorization": f"Bearer {LANCHAT_TOKEN}"}
    
    # Определяем тип файла
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
        
        async with aiohttp.ClientSession() as session:
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


# ==================== ПОЛУЧЕНИЕ ИЗ LANCHAT ====================

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
    
    # Стикеры и вложения
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
    
    # Реакции
    reactions = msg_data.get("reactions", [])
    reactions_text = ""
    if reactions:
        reactions_list = [f"{r.get('emoji', '')} {r.get('count', 0)}" for r in reactions if r.get('count', 0) > 0]
        if reactions_list:
            reactions_text = f"\n\n📊 Реакции: {' '.join(reactions_list)}"
    
    # Сборка
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
        if chat_id != LANCHAT_CHAT_ID:
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


# ==================== ОТПРАВКА ИЗ TELEGRAM ====================

async def handle_telegram_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик сообщений из Telegram"""
    if str(update.effective_chat.id) != TELEGRAM_CHAT_ID:
        await update.message.reply_text("❌ Доступ запрещен.")
        return
    
    message = update.message
    text = message.text
    
    if text and text.startswith('/'):
        return
    
    if text:
        # Проверяем ответ на сообщение из LanChat
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
            await update.message.reply_text("❌ Ошибка отправки")


# ==================== TELEGRAM БОТ ====================

async def start_telegram_bot():
    """Запуск Telegram бота"""
    global telegram_bot
    
    application = Application.builder().token(TELEGRAM_TOKEN).build()
    telegram_bot = application.bot
    
    # Команды
    @application.add_handler(CommandHandler("start"))
    async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
        if str(update.effective_chat.id) != TELEGRAM_CHAT_ID:
            await update.message.reply_text("❌ Доступ запрещен.")
            return
        
        status_text = (
            "🤖 <b>LanChat ↔ Telegram Бот</b>\n\n"
            f"✅ Статус: <b>Активен</b>\n"
            f"📡 Чат LanChat: <code>{LANCHAT_CHAT_ID}</code>\n"
            f"🔄 <b>Двусторонняя синхронизация</b>\n\n"
            "📤 LanChat → Telegram: Автоматически\n"
            "📥 Telegram → LanChat: Отправьте сообщение\n"
            "💬 Ответ на сообщение → ответ в LanChat\n"
            "📎 Медиа → пересылается в LanChat\n\n"
            "Команды:\n"
            "/status - Статус бота\n"
            "/ping - Проверка соединения\n"
            "/help - Помощь"
        )
        await update.message.reply_text(status_text, parse_mode="HTML")
    
    @application.add_handler(CommandHandler("status"))
    async def status_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
        if str(update.effective_chat.id) != TELEGRAM_CHAT_ID:
            await update.message.reply_text("❌ Доступ запрещен.")
            return
        
        status = "✅ Подключен" if lanchat_client and lanchat_client._running else "❌ Отключен"
        status_text = (
            "📊 <b>Статус бота</b>\n\n"
            f"LanChat: {status}\n"
            f"Chat ID: <code>{LANCHAT_CHAT_ID}</code>\n"
            f"Обработано: <b>{len(processed_messages)}</b>\n"
            f"Подписанных чатов: <b>{len(lanchat_client.subscribed_chats) if lanchat_client else 0}</b>\n"
            f"🔄 <b>Режим: Двусторонний</b>"
        )
        await update.message.reply_text(status_text, parse_mode="HTML")
    
    @application.add_handler(CommandHandler("ping"))
    async def ping_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
        if str(update.effective_chat.id) != TELEGRAM_CHAT_ID:
            await update.message.reply_text("❌ Доступ запрещен.")
            return
        await update.message.reply_text("🏓 Pong!")
    
    @application.add_handler(CommandHandler("help"))
    async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
        if str(update.effective_chat.id) != TELEGRAM_CHAT_ID:
            await update.message.reply_text("❌ Доступ запрещен.")
            return
        
        help_text = (
            "📖 <b>Помощь</b>\n\n"
            "🔄 <b>Двусторонняя синхронизация</b>\n\n"
            "1️⃣ <b>LanChat → Telegram</b>\n"
            "   - Все сообщения из LanChat автоматически пересылаются\n"
            "   - Поддерживаются: текст, стикеры, вложения, реакции\n\n"
            "2️⃣ <b>Telegram → LanChat</b>\n"
            "   - Отправьте текст → появится в LanChat\n"
            "   - Ответьте на сообщение бота → ответ в LanChat\n"
            "   - Отправьте фото/документ → отправится в LanChat\n\n"
            "Команды:\n"
            "/start - Старт\n"
            "/status - Статус\n"
            "/ping - Проверка\n"
            "/help - Помощь"
        )
        await update.message.reply_text(help_text, parse_mode="HTML")
    
    # Обработчики сообщений (Telegram → LanChat)
    application.add_handler(MessageHandler(
        filters.TEXT & ~filters.COMMAND & filters.Chat(chat_id=int(TELEGRAM_CHAT_ID)),
        handle_telegram_message
    ))
    
    application.add_handler(MessageHandler(
        (filters.PHOTO | filters.DOCUMENT | filters.VIDEO | filters.AUDIO) & filters.Chat(chat_id=int(TELEGRAM_CHAT_ID)),
        send_media_to_lanchat
    ))
    
    # Запуск
    await application.initialize()
    await application.start()
    await application.updater.start_polling()
    
    while True:
        await asyncio.sleep(1)


# ==================== MAIN ====================

async def main():
    global lanchat_client
    
    logger.info("🚀 Запуск LanChat ↔ Telegram бота")
    logger.info("🔄 Режим: ДВУСТОРОННЯЯ СИНХРОНИЗАЦИЯ")
    
    lanchat_client = LanChatClient(LANCHAT_TOKEN, LANCHAT_WS_URL)
    lanchat_client.on_message(handle_lanchat_message)
    
    if await lanchat_client.connect():
        await lanchat_client.subscribe(LANCHAT_CHAT_ID)
        asyncio.create_task(lanchat_client.listen())
        logger.info("✅ LanChat клиент запущен")
    
    await start_telegram_bot()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("👋 Бот остановлен")
    except Exception as e:
        logger.error(f"❌ Критическая ошибка: {e}")
        sys.exit(1)