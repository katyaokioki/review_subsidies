import json
import os
import time
from loguru import logger
import pika
from to_db import ExcelLoader, Updated
import sys
import pandas as pd

logger.remove()  # убрать предустановленный sink
logger.add(sys.stderr, level="INFO")  # общий уровень
# Если используете SQLAlchemy, скрыть его подробный вывод:
logger.add(sys.stderr, level="WARNING", filter=lambda r: r["name"] == "sqlalchemy.engine")


DOWNLOAD_DIR = "/app/downloads"

# logger = logging.getLogger("ReadingService")
# logger.setLevel(logging.INFO)
# console_handler = logging.StreamHandler()
# console_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
# logger.addHandler(console_handler)

class Rabbit_to_db:
    def __init__(self, max_retries=5, retry_delay=5):
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.connection = None
        self.channel = None
        self.consumer_tag = None
        self._connect()
        self._declare_and_consume()

    def _connect(self):
        attempt = 0
        while attempt < self.max_retries:
            try:
                self.connection = pika.BlockingConnection(
                    pika.ConnectionParameters(
                        host="rabbitmq",
                        heartbeat=180,
                        blocked_connection_timeout=300,
                    )
                )
                self.channel = self.connection.channel()
                self.channel.basic_qos(prefetch_count=1)
                self.channel.queue_declare(queue="bot_db", durable=True)
                self.channel.queue_declare(queue="codes", durable=True)
                logger.info("Успешно подключились к RabbitMQ")
                return
            except pika.exceptions.AMQPConnectionError as e:
                attempt += 1
                wait_time = self.retry_delay * (2 ** (attempt - 1))
                logger.error(f"Попытка подключения {attempt}/{self.max_retries} не удалась: {e}. Повтор через {wait_time} сек.")
                time.sleep(wait_time)
        logger.critical("Не удалось подключиться к RabbitMQ после нескольких попыток")
        raise Exception("Не удалось подключиться к RabbitMQ после нескольких попыток")

    def _declare_and_consume(self):
        if self.consumer_tag:
            try:
                self.channel.basic_cancel(self.consumer_tag)
            except Exception:
                pass
        self.consumer_tag = self.channel.basic_consume(
            queue="bot_db",
            on_message_callback=self.callback,
            auto_ack=False,
        )
        logger.info("Ожидание сообщений в очереди bot_db...")

    def process_excel(self, path):
        logger.remove()
        logger.add(sys.stderr, level="INFO")
        logger.add("/app/logs/app.log", rotation="10 MB", retention="7 days", encoding="utf-8", level="INFO")



        waited, delay = 0.0, 0.5
        while not os.path.isfile(path) and waited < 180:
            time.sleep(delay)
            waited += delay
            delay = min(delay * 2, 8)
        if not os.path.isfile(path):
            logger.warning(f"Файл не найден: {path}")
            return

        try:

            logger.info(f"Начинаю обработку: {path}")

            dfs = pd.read_excel(path, sheet_name=None)
            combined = pd.concat(dfs, names=["sheet"]).reset_index(level=0).rename(columns={"level_0": "sheet"})
            loader = ExcelLoader()
            df = loader.normalize_code_column(combined)
            up = Updated()
            if up.check():
                codes = up.db(df)
            else:
                up.create_table()
                codes = up.db(df)


            df["end_date_dt"] = pd.to_datetime(df["end_date"], format="%d.%m.%Y %H:%M", errors="coerce")
            now = pd.Timestamp.now()
            if len(codes) == 0:
                logger.info('нет новых субсидий')
            else:
                for code in codes:
                    row = df.loc[df["code"] == code]
                    is_after = (row["end_date_dt"] > now).any()
                    up.isactive(is_after, code)
                    if is_after:

                        self.channel.basic_publish(exchange='', routing_key='codes', body=str(code))
                        logger.info(f"Отправлен код: {code}")
                
            

            


        except Exception as e:
            logger.error(f"Ошибка чтения Excel или записи в БД: {e}")

    def callback(self, ch, method, properties, body):
        try:
            message = json.loads(body.decode())
            logger.info(f"Получено сообщение из bot_db: {message}")
            if message.get("status") == "downloaded" and "file_path" in message:
                self.process_excel(message['file_path'])
            else:
                logger.warning(f"Сообщение пропущено: {message}")
            ch.basic_ack(delivery_tag=method.delivery_tag)
        except pika.exceptions.AMQPConnectionError as e:
            logger.error(f"Ошибка AMQPConnectionError в callback: {e}. Переподключение...")
            self._connect()
        except Exception as e:
            logger.error(f"Ошибка в callback: {e}")
        
        # except pika.exceptions.AMQPConnectionError as e:
        #     logger.error(f"Ошибка AMQPConnectionError в callback: {e}. Переподключение...")
        #     try:
        #         ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
        #     except Exception:
        #         pass
        #     self._connect()
        #     self._declare_and_consume()
        # except Exception as e:
        #     logger.error(f"Ошибка в callback: {e}")
        #     try:
        #         ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
        #     except Exception:
        #         pass

    def start_listening(self):
        while True:
            try:
                self.channel.start_consuming()
            except pika.exceptions.StreamLostError as e:
                logger.error(f"Соединение потеряно: {e}. Переподключение...")
                self._connect()
                self._declare_and_consume()
            except pika.exceptions.AMQPConnectionError as e:
                logger.error(f"AMQP ошибка: {e}. Переподключение...")
                self._connect()
                self._declare_and_consume()
            except KeyboardInterrupt:
                try:
                    if self.consumer_tag:
                        self.channel.basic_cancel(self.consumer_tag)
                finally:
                    break
            except Exception as e:
                logger.error(f"Неожиданная ошибка: {e}")
                time.sleep(5)

if __name__ == "__main__":
    time.sleep(10)
    service = Rabbit_to_db()
    service.start_listening()
