import json
import os
import pandas as pd
import pika
import time
import logging

# Настройка логирования
logger = logging.getLogger("ReadingService")
logger.setLevel(logging.INFO)

# Форматирование логов
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

# Вывод в консоль
console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

# При желании можно добавить файл для логов
# file_handler = logging.FileHandler('reading_service.log')
# file_handler.setFormatter(formatter)
# logger.addHandler(file_handler)

class ReadingService:
    def __init__(self, max_retries=5, retry_delay=5):
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.connection = None
        self.channel = None
        self._connect()
    
    def _connect(self):
        attempt = 0
        while attempt < self.max_retries:
            try:
                self.connection = pika.BlockingConnection(
                    pika.ConnectionParameters('rabbitmq', heartbeat=60)
                )
                self.channel = self.connection.channel()
                self.channel.queue_declare(queue='excel_status', durable=True)
                self.channel.queue_declare(queue='codes', durable=True)
                logger.info("Успешно подключились к RabbitMQ")
                return
            except pika.exceptions.AMQPConnectionError as e:
                attempt += 1
                wait_time = self.retry_delay * (2 ** (attempt - 1))
                logger.error(f"Попытка подключения {attempt}/{self.max_retries} не удалась: {e}. Повтор через {wait_time} сек.")
                time.sleep(wait_time)
        logger.critical("Не удалось подключиться к RabbitMQ после нескольких попыток")
        raise Exception("Не удалось подключиться к RabbitMQ после нескольких попыток")

    def process_excel(self, file_name):
        file_path = os.path.join('downloads', file_name)
        if not os.path.isfile(file_path):
            logger.warning(f"Файл не найден: {file_path}")
            return

        try:
            df = pd.read_excel(file_path, sheet_name='2025 - 2027').head(10)
            codes = df['Шифр отбора'].tolist()
            for code in codes:
                self.channel.basic_publish(exchange='', routing_key='codes', body=str(code))
                logger.info(f"Отправлен код: {code}")
        except Exception as e:
            logger.error(f"Ошибка чтения Excel или отправки данных: {e}")

    def callback(self, ch, method, properties, body):
        try:
            message = json.loads(body.decode())
            logger.info(f"Получено сообщение из excel_status: {message}")

            if message.get("status") == "downloaded" and "file_name" in message:
                self.process_excel(message["file_name"])

            ch.basic_ack(delivery_tag=method.delivery_tag)
        except pika.exceptions.AMQPConnectionError as e:
            logger.error(f"Ошибка AMQPConnectionError в callback: {e}. Переподключение...")
            self._connect()
        except Exception as e:
            logger.error(f"Ошибка в callback: {e}")

    def start_listening(self):
        self.channel.basic_qos(prefetch_count=1)
        while True:
            try:
                self.channel.basic_consume(queue='excel_status', on_message_callback=self.callback, auto_ack=False)
                logger.info("Ожидание сообщений в очереди excel_status...")
                self.channel.start_consuming()
            except pika.exceptions.AMQPConnectionError as e:
                logger.error(f"Обрыв соединения с RabbitMQ: {e}. Пытаюсь переподключиться...")
                self._connect()
            except Exception as e:
                logger.error(f"Неожиданная ошибка: {e}")
                time.sleep(5)

if __name__ == '__main__':
    time.sleep(10)  # Дать время RabbitMQ запуститься
    service = ReadingService()
    service.start_listening()
