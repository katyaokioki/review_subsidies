from pika import BlockingConnection, ConnectionParameters, BasicProperties
from pika.exceptions import AMQPConnectionError
from loguru import logger

connection_params = ConnectionParameters(host='localhost')

def main():
    with BlockingConnection(connection_params) as connection:
        with connection.channel() as ch: 
            ch.queue_declare(queue='hello')

            ch.basic_publish(exchange='', routing_key='hello', body='Hello, World!')
            logger.info("Message sent")

if __name__ == '__main__':
    main()