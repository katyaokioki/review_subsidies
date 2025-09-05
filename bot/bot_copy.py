from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, MessageHandler, filters, ContextTypes
import os
import pika
import json
import logging
import time
from sqlalchemy import create_engine, Column, String
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy.dialects.postgresql import insert
import pandas as pd

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
Base = declarative_base()

DOWNLOAD_DIR = "downloads"
TOKEN = os.getenv("TELEGRAM_TOKEN", "7585624163:AAEktpBHYvVGWL6iORmFthg6W0F76IXm7DY")

os.makedirs(DOWNLOAD_DIR, exist_ok=True)

class RabbitMQ:
    def __init__(self, max_retries=5, retry_delay=3):
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.connection = None
        self.channel = None
        self._connect()

    def _connect(self):
        for attempt in range(self.max_retries):
            try:
                self.connection = pika.BlockingConnection(
                    pika.ConnectionParameters(
                        'rabbitmq',
                        heartbeat=60,
                        blocked_connection_timeout=300
                    )
                )
                self.channel = self.connection.channel()
                self.channel.queue_declare(queue='excel_status', durable=True)
                logger.info("✅ Успешно подключились к RabbitMQ")
                return
            except pika.exceptions.AMQPConnectionError as e:
                logger.error(f"❌ Попытка {attempt + 1}/{self.max_retries}: {e}")
                if attempt < self.max_retries - 1:
                    time.sleep(self.retry_delay)
                else:
                    logger.error("❌ Не удалось подключиться к RabbitMQ")
                    raise

    def send_message(self, message):
        try:
            if self.connection is None or self.connection.is_closed:
                self._connect()
            self.channel.basic_publish(
                exchange='',
                routing_key='excel_status',
                body=json.dumps(message, ensure_ascii=False),
                properties=pika.BasicProperties(delivery_mode=2)
            )
            logger.info(f"📤 Сообщение отправлено в очередь: {message}")
            return True
        except Exception as e:
            logger.error(f"❌ Ошибка при отправке сообщения: {e}")
            return False

    def close(self):
        if self.connection and not self.connection.is_closed:
            self.connection.close()
            logger.info("🔒 Соединение с RabbitMQ закрыто")

class Bot:
    def __init__(self):
        self.bot = ApplicationBuilder().token(TOKEN).build()
        self.rabbitmq = RabbitMQ()
        self._setup_handlers()

    def _setup_handlers(self):
        self.bot.add_handler(CommandHandler("start", self.start_command))
        self.bot.add_handler(
            MessageHandler(filters.Document.ALL | filters.PHOTO, self.handle_file)
        )

    async def start_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        welcome_message = (
            "🤖 Привет! Я бот для обработки файлов.\n"
            "📎 Отправь мне файл или фотографию, и я сохраню их локально "
            "и отправлю уведомление в очередь обработки."
        )
        await update.message.reply_text(welcome_message)

    async def handle_file(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        message = update.message
        try:
            if message.document:
                file = await context.bot.get_file(message.document.file_id)
                file_name = message.document.file_name
                file_type = "document"
            elif message.photo:
                file = await context.bot.get_file(message.photo[-1].file_id)
                file_name = f"photo_{message.photo[-1].file_id}.jpg"
                file_type = "photo"
            else:
                await update.message.reply_text("❌ Пожалуйста, пришлите файл или фотографию.")
                return
            file_path = os.path.join(DOWNLOAD_DIR, file_name)
            await file.download_to_drive(file_path)
            await update.message.reply_text(f"✅ Файл сохранён: {file_name}\n📁 Путь: {file_path}")
            
            rabbit_message = {
                "user_id": message.from_user.id,
                "username": message.from_user.username or "unknown",
                "file_name": file_name,
                "file_type": file_type,
                "file_path": file_path,
                "status": "downloaded",
                "timestamp": message.date.isoformat()
            }
            success = self.rabbitmq.send_message(rabbit_message)
            if success:
                await update.message.reply_text("📤 Уведомление отправлено в очередь обработки")
            else:
                await update.message.reply_text("⚠️ Файл сохранён, но возникла проблема с очередью")
        except Exception as e:
            logger.error(f"❌ Ошибка при обработке файла: {e}")
            await update.message.reply_text(f"❌ Произошла ошибка при обработке файла: {str(e)}")

    def run(self):
        try:
            logger.info("🚀 Запуск Telegram бота...")
            self.bot.run_polling()
        except KeyboardInterrupt:
            logger.info("⏹️ Бот остановлен пользователем")
        except Exception as e:
            logger.error(f"❌ Критическая ошибка: {e}")
        finally:
            self.rabbitmq.close()

    
if __name__ == "__main__":
    time.sleep(5)  # ожидание запуска RabbitMQ
    bot = Bot()
    bot.run()
    Base = declarative_base()
