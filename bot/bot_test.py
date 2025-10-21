import os
import json
import time
from loguru import logger
from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, MessageHandler, ContextTypes, filters
import pika
import threading
import asyncio


# logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
# logger = logging.getLogger(__name__)

DOWNLOAD_DIR = os.environ.get("DOWNLOAD_DIR", "/app/downloads")
TOKEN = os.getenv("TELEGRAM_TOKEN", "7585624163:AAEihx-v-JuPVlpQZyGB4xG5agTaKBC2VIQ")
RABBIT_HOST = os.getenv("RABBIT_HOST", "rabbitmq")
RABBIT_DB = os.getenv("RABBIT_DB", "bot_db")           # очередь для сервиса записи в БД


os.makedirs(DOWNLOAD_DIR, exist_ok=True)

SEND_EMAIL_QUEUE = "send_email"




class RabbitMQ:
    def __init__(self, host=RABBIT_HOST, bot_db=RABBIT_DB, max_retries=5, retry_delay=3):
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.host = host
        self.bot_db = bot_db
        self.connection = None
        self.channel = None
        self._connect()

    def _connect(self):
        for attempt in range(self.max_retries):
            try:
                self.connection = pika.BlockingConnection(
                    pika.ConnectionParameters(
                        host=self.host,
                        heartbeat=180,
                        blocked_connection_timeout=300,
                    )
                )
                self.channel = self.connection.channel()
                # Объявляем обе очереди один раз (идемпотентно)
                self.channel.queue_declare(queue=self.bot_db, durable=True)
               
                # self.channel.queue_declare(queue=self.queue_reading, durable=True)
                logger.info("✅ Успешно подключились к RabbitMQ и объявили очереди")
                return
            except pika.exceptions.AMQPConnectionError as e:
                logger.error(f"❌ Попытка {attempt + 1}/{self.max_retries}: {e}")
                if attempt < self.max_retries - 1:
                    time.sleep(self.retry_delay)
                else:
                    logger.error("❌ Не удалось подключиться к RabbitMQ")
                    raise

    def send_to_both(self, message: dict) -> bool:
        """Публикует сообщение в обе очереди: db и reading."""
        try:
            if self.connection is None or self.connection.is_closed:
                self._connect()
            payload = json.dumps(message, ensure_ascii=False)
        
            self.channel.basic_publish(
                exchange="",
                routing_key=self.bot_db,
                body=payload,
                properties=pika.BasicProperties(delivery_mode=2),  # persistent
            )
            logger.info(f"📤 Отправлено в очередь bot_db: {message}")
            return True
        except Exception as e:
            logger.error(f"❌ Ошибка при отправке сообщения в очереди: {e}")
            return False



    def close(self):
        if self.connection and not self.connection.is_closed:
            self.connection.close()
            logger.info("🔒 Соединение с RabbitMQ закрыто")

class Bot:
    def __init__(self):
        self.app = ApplicationBuilder().token(TOKEN).build()
        self.rabbitmq = RabbitMQ()
        self._setup_handlers()
        self.TARGET_NAME = "Реестр отборов.xlsx"

    def _setup_handlers(self):
        self.app.add_handler(CommandHandler("start", self.start_command))
        self.app.add_handler(MessageHandler(filters.Document.ALL | filters.PHOTO, self.handle_file))

    async def start_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        msg = "🤖 Пришлите Excel/CSV файл — сохраню и отправлю на обработку."
        await update.message.reply_text(msg)

    async def handle_file(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        message = update.message
        try:
            if message.document:
                tg_file = await context.bot.get_file(message.document.file_id)
                orig_name = message.document.file_name or "file.bin"
                file_type = "document"
            elif message.photo:
                tg_file = await context.bot.get_file(message.photo[-1].file_id)
                orig_name = f"photo_{message.photo[-1].file_id}.jpg"
                file_type = "photo"
            else:
                await update.message.reply_text("❌ Пожалуйста, пришлите файл или фотографию.")
                return
        

            safe_name = os.path.basename(orig_name).strip()
            # file_path = os.path.normpath(os.path.join(DOWNLOAD_DIR, safe_name))
            # abs_path = os.path.abspath(file_path)

            target_path = os.path.join(DOWNLOAD_DIR, self.TARGET_NAME)
            abs_path = os.path.abspath(target_path)

           


            await tg_file.download_to_drive(abs_path)
            await update.message.reply_text(f"✅ Файл сохранён: {safe_name}\n📁 Путь: {abs_path}")

            rabbit_message = {
                "user_id": message.from_user.id,
                "username": message.from_user.username or "unknown",
                "file_name": safe_name,
                "file_type": file_type,
                "file_path": abs_path,  # абсолютный путь для consumer
                "status": "downloaded",
                "timestamp": message.date.isoformat(),
            }

            success = self.rabbitmq.send_to_both(rabbit_message)
            if success:
                await update.message.reply_text("📤 Уведомление отправлено в очереди: db и reading")
            else:
                await update.message.reply_text("⚠️ Файл сохранён, но возникла проблема с очередями")
        except Exception as e:
            logger.error(f"❌ Ошибка при обработке файла: {e}")
            await update.message.reply_text(f"❌ Произошла ошибка: {str(e)}")

    def run(self):
        try:
            logger.info("🚀 Запуск Telegram бота...")
            self.app.run_polling()
        finally:
            self.rabbitmq.close()

if __name__ == "__main__":
    time.sleep(5)
    bot = Bot()
    bot.run()
