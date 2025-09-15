import logging
import os
from confluent_kafka import Consumer


KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
KAFKA_TOPIC = "radar_trades"
KAFKA_GROUP_ID = "clickhouse-consumers-v3"


LOG_LEVEL = logging.INFO
BATCH_SIZE = 200
BATCH_TIMEOUT_SECONDS = 5

logging.basicConfig(level=LOG_LEVEL, format='%(asctime)s - %(levelname)s - %(message)s')

def parse_and_transform(message_value):
    return message_value

def main():
    """Main function to consume from Kafka and insert into ClickHouse."""

    consumer_conf = {
        'bootstrap.servers': KAFKA_BROKER,
        'group.id': KAFKA_GROUP_ID,
        'auto.offset.reset': 'earliest'
    }
    consumer = Consumer(consumer_conf)
    consumer.subscribe([KAFKA_TOPIC])
    logging.info(f"Subscribed to Kafka topic '{KAFKA_TOPIC}'.")


    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            print(msg)

    except KeyboardInterrupt:
        logging.info("Shutting down consumer.")
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")
    finally:
        consumer.close()
        logging.info("Kafka consumer closed.")

if __name__ == "__main__":
    main()

