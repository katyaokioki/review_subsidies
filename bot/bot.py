import os
import json
import time
import logging
import pandas as pd
from typing import List, Dict

from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, MessageHandler, ContextTypes, filters

import pika
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError
from sqlalchemy.dialects.postgresql import insert

# ----------------------------------
# ЛОГИРОВАНИЕ
# ----------------------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# ----------------------------------
# НАСТРОЙКИ
# ----------------------------------
DOWNLOAD_DIR = os.environ.get("DOWNLOAD_DIR", "downloads")
TOKEN = os.getenv("TELEGRAM_TOKEN", "7585624163:AAEktpBHYvVGWL6iORmFthg6W0F76IXm7DY")

# DSN для контейнерной БД: host = имя сервиса compose: postgres
DB_DSN = os.getenv("POSTGRES_DSN", "postgresql+psycopg://postgres:1234@postgres:5432/subsidies")

RABBIT_HOST = os.getenv("RABBIT_HOST", "rabbitmq")
RABBIT_QUEUE = os.getenv("RABBIT_QUEUE", "excel_status")

TABLE_NAME = os.getenv("TARGET_TABLE", "main_table")
UNIQUE_KEY = os.getenv("UNIQUE_KEY", "Шифр отбора")  # ключ конфликта
# колонки, которые нужно апсертить (кроме ключа)
# UPSERT_COLUMNS = os.getenv("UPSERT_COLUMNS", "")  # "Колонка1,Колонка2"
# UPSERT_COLUMNS = [c.strip() for c in UPSERT_COLUMNS.split(",") if c.strip()]

os.makedirs(DOWNLOAD_DIR, exist_ok=True)

# # SQLAlchemy engine с pre_ping для устойчивости
# engine = create_engine(
#     DB_DSN,
#     future=True,
#     pool_pre_ping=True,  # проверка соединения перед использованием
# )

# ----------------------------------
# # ВСПОМОГАТЕЛЬНОЕ: ожидание готовности БД
# # ----------------------------------
# def wait_for_db(max_attempts: int = 30, delay: float = 2.0):
#     for attempt in range(1, max_attempts + 1):
#         try:
#             with engine.connect() as conn:
#                 conn.execute(text("SELECT 1"))
#             logger.info("✅ Подключение к БД успешно")
#             return
#         except OperationalError as e:
#             logger.warning(f"⏳ Ожидание БД (попытка {attempt}/{max_attempts}): {e}")
#             time.sleep(delay)
#     raise RuntimeError("База данных недоступна после ожидания")

# # ----------------------------------
# # UPSERT ИЗ DATAFRAME В PostgreSQL
# # ----------------------------------
# def _upsert_method(table, conn, keys, data_iter):
#     rows = [dict(zip(keys, row)) for row in data_iter]
#     if not rows:
#         return
#     stmt = insert(table.table).values(rows)

#     # Если список колонок не задан, обновляем все, кроме ключа
#     if UPSERT_COLUMNS:
#         update_cols = {col: stmt.excluded[col] for col in UPSERT_COLUMNS if col != UNIQUE_KEY}
#     else:
#         update_cols = {c.name: stmt.excluded[c.name] for c in table.table.c if c.name != UNIQUE_KEY}

#     stmt = stmt.on_conflict_do_update(
#         index_elements=[UNIQUE_KEY],  # при именованном ограничении можно constraint="..."
#         set_=update_cols,
#     )
#     conn.execute(stmt)

def read_to_dataframe(path: str) -> pd.DataFrame:
    if path.lower().endswith((".xlsx", ".xls")):
        # можно уточнить sheet_name=0 и dtype=...
        df = pd.read_excel(path)
    else:
        df = pd.read_csv(path)
    return df

def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    # Часто Excel/CSV несут пробелы; приведём к точным названиям как есть
    # Если нужно — можно тут маппить русские заголовки к английским.
    return df

# def upsert_file_to_db(file_path: str):
#     df = read_to_dataframe(file_path)
#     df = normalize_columns(df)

#     if UNIQUE_KEY not in df.columns:
#         raise ValueError(f"В файле отсутствует обязательная колонка '{UNIQUE_KEY}'")

#     # Если UPSERT_COLUMNS задан, оставим только необходимые столбцы + ключ
#     if UPSERT_COLUMNS:
#         keep = [c for c in UPSERT_COLUMNS if c in df.columns]
#         if UNIQUE_KEY not in keep:
#             keep.append(UNIQUE_KEY)
#         df = df[keep]

#     with engine.begin() as conn:
#         df.to_sql(
#             name=TABLE_NAME,
#             con=conn,
#             if_exists="append",
#             index=False,
#             method=_upsert_method,
#             chunksize=1000,
#         )

# ----------------------------------
# RABBITMQ-ОБОЛОЧКА
# ----------------------------------
class RabbitMQ:
    def __init__(self, host=RABBIT_HOST, queue=RABBIT_QUEUE, max_retries=5, retry_delay=3):
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.host = host
        self.queue = queue
        self.connection = None
        self.channel = None
        self._connect()

    def _connect(self):
        for attempt in range(self.max_retries):
            try:
                self.connection = pika.BlockingConnection(
                    pika.ConnectionParameters(
                        self.host,
                        heartbeat=60,
                        blocked_connection_timeout=300
                    )
                )
                self.channel = self.connection.channel()
                self.channel.queue_declare(queue=self.queue, durable=True)
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
                routing_key=self.queue,
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

# ----------------------------------
# TELEGRAM-БОТ
# ----------------------------------
class Bot:
    def __init__(self):
        self.app = ApplicationBuilder().token(TOKEN).build()
        self.rabbitmq = RabbitMQ()
        self._setup_handlers()

    def _setup_handlers(self):
        self.app.add_handler(CommandHandler("start", self.start_command))
        self.app.add_handler(MessageHandler(filters.Document.ALL | filters.PHOTO, self.handle_file))

    async def start_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        welcome_message = (
            "🤖 Привет! Я бот для обработки файлов.\n"
            f"📎 Отправь файл — сохраню и загружу данные в БД '{TABLE_NAME}' "
            f"(UPSERT по '{UNIQUE_KEY}')."
        )
        await update.message.reply_text(welcome_message)

    async def handle_file(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        message = update.message
        try:
            if message.document:
                tg_file = await context.bot.get_file(message.document.file_id)
                file_name = message.document.file_name
                file_type = "document"
            elif message.photo:
                tg_file = await context.bot.get_file(message.photo[-1].file_id)
                file_name = f"photo_{message.photo[-1].file_id}.jpg"
                file_type = "photo"
            else:
                await update.message.reply_text("❌ Пожалуйста, пришлите файл или фотографию.")
                return

            file_path = os.path.join(DOWNLOAD_DIR, file_name)
            await tg_file.download_to_drive(file_path)
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
            self.app.run_polling()
        except KeyboardInterrupt:
            logger.info("⏹️ Бот остановлен пользователем")
        except Exception as e:
            logger.error(f"❌ Критическая ошибка: {e}")
        finally:
            self.rabbitmq.close()

# ----------------------------------
# ТОЧКА ВХОДА
# ----------------------------------
if __name__ == "__main__":
    # Небольшая задержка для старта RabbitMQ (если без healthcheck)
    time.sleep(5)
    bot = Bot()
    bot.run()
