# requirements:
#   pika==1.3.2
#   aiogram==3.*
import asyncio
import json
import os
import pika
from aiogram import Bot

TELEGRAM_TOKEN = os.environ["TELEGRAM_TOKEN", "7585624163:AAEihx-v-JuPVlpQZyGB4xG5agTaKBC2VIQ"]
TELEGRAM_CHAT_ID = os.environ["TELEGRAM_CHAT_ID"]  # пример: -1001234567890

RABBITMQ_URL = os.environ.get("RABBITMQ_URL", "amqp://guest:guest@rabbitmq:5672/%2F")
QUEUE_NAME = os.environ.get("QUEUE_NAME", "notifications")

bot = Bot(token=TELEGRAM_TOKEN)

def build_text(msg: dict) -> str:
    # Пример правила формирования текста
    title = msg.get("title", "Событие")
    level = msg.get("level", "info")
    body = msg.get("body", "")
    return f"[{level.upper()}] {title}\n{body}"

async def send_to_telegram(text: str) -> None:
    await bot.send_message(chat_id=TELEGRAM_CHAT_ID, text=text)

def on_message(channel, method, properties, body: bytes):
    try:
        payload = json.loads(body.decode("utf-8"))
        text = build_text(payload)
        # Запускаем отправку в отдельном event loop (либо держим единый loop в фоновой задаче)
        asyncio.run(send_to_telegram(text))
        channel.basic_ack(delivery_tag=method.delivery_tag)
    except Exception:
        # В реальном коде — логирование и dead-letter; можно requeue=False для DLX
        channel.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

def main():
    params = pika.URLParameters(RABBITMQ_URL)
    connection = pika.BlockingConnection(params)
    channel = connection.channel()
    # durable: очередь переживает рестарт брокера
    channel.queue_declare(queue=QUEUE_NAME, durable=True)
    # Prefetch 1 для справедливого распределения
    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue=QUEUE_NAME, on_message_callback=on_message, auto_ack=False)
    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        channel.stop_consuming()
    finally:
        connection.close()

if __name__ == "__main__":
    main()
