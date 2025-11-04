import logging
import os
import json
import hashlib
import argparse
import time
import threading

from datetime import datetime
from decimal import Decimal
from clickhouse_driver import Client
from confluent_kafka import Consumer, KafkaException
from dotenv import load_dotenv

load_dotenv()

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "radar_trades")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "clickhouse-consumers-v3")

CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "localhost")
CLICKHOUSE_PORT = os.getenv("CLICKHOUSE_PORT", "9000")
CLICKHOUSE_DB = os.getenv("CLICKHOUSE_DB", "trades")
CLICKHOUSE_TABLE = os.getenv("CLICKHOUSE_TABLE", "trades_master")
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "default")

LOG_LEVEL = getattr(logging, os.getenv("LOG_LEVEL", "INFO").upper(), logging.INFO)
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "200"))
BATCH_TIMEOUT_SECONDS = int(os.getenv("BATCH_TIMEOUT_SECONDS", "5"))

logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s - %(levelname)s - %(message)s")


def get_clickhouse_client():
    """Establishes a resilient connection to ClickHouse on the correct native port."""
    try:
        client = Client(
            host=CLICKHOUSE_HOST,
            port=9000,
            connect_timeout=10,
            user=CLICKHOUSE_USER,
            password=CLICKHOUSE_PASSWORD,
        )
        client.execute("SELECT 1")
        logging.info("Successfully connected to ClickHouse.")
        return client
    except Exception as e:
        logging.critical(
            f"Failed to connect to ClickHouse after several retries. Detail: {e}"
        )
        raise


def restart_database():
    client = get_clickhouse_client()
    client.execute(f"DROP DATABASE IF EXISTS {CLICKHOUSE_DB}")
    client.execute(f"DROP TABLE IF EXISTS {CLICKHOUSE_TABLE}")


def setup_database(client):
    """
    Creates the database and a unified table using ReplacingMergeTree for deduplication.
    """
    try:
        logging.info(f"Creating database '{CLICKHOUSE_DB}' if not exists...")
        client.execute(f"CREATE DATABASE IF NOT EXISTS {CLICKHOUSE_DB}")

        logging.info(f"Creating table '{CLICKHOUSE_TABLE}' if not exists...")
        client.execute(
            f"""
        CREATE TABLE IF NOT EXISTS {CLICKHOUSE_DB}.{CLICKHOUSE_TABLE} (
            trade_id        String,
            source          String,
            symbol          String,
            side            String,
            price           Float64,
            base_amount     Float64,
            quote_amount    Float64,
            event_time      DateTime,
            inserted_at     DateTime DEFAULT now()
        ) ENGINE = ReplacingMergeTree()
        ORDER BY (source, trade_id)
        """
        )
        logging.info("Database and unified table setup complete.")
    except Exception as e:
        logging.error(f"Error during database setup: {e}")
        return

def transform_kafka_to_row(data):
    exchange = data.get("exchange")
    symbol = data.get("symbol")
    side = data.get("side")
    price = data.get("price")
    volume = data.get("volume")
    quantity = data.get("quantity")
    time_of_trade = data.get("time")
    trade_id = data.get("id")
    if not trade_id:
        unique_string = f"{exchange}-{symbol}-{time_of_trade}-{price}-{volume}-{side}"
        trade_id = hashlib.sha1(unique_string.encode("utf-8")).hexdigest()

    row = (
        trade_id,
        exchange,
        symbol,
        side,
        float(price),
        float(volume),
        quantity,
        datetime.fromisoformat(time_of_trade),
    )
    return row  # Return as a list for consistency


def proccess_kafka_data(data):
    try:
        if isinstance(data, list):
            transformed_data = []
            for d in data:
                transformed_data.append(transform_kafka_to_row(d))
            return transformed_data
        else:
            return [transform_kafka_to_row(data)]
    except Exception as e:
        logging.error(f"Failed to transform data. Error: {e}.")
        return []


def parse_and_transform(message_value):
    """
    Inspects the message format and routes it to the correct transformation function.
    """
    try:
        data = json.loads(message_value)
        return proccess_kafka_data(data)
    except Exception as e:
        logging.error(f"Failed to parse JSON. Error: {e}.")
        return []


def consumer_worker(worker_id, stop_event):
    client = get_clickhouse_client()
    consumer = Consumer(
        {
            "bootstrap.servers": KAFKA_BROKER,
            "group.id": KAFKA_GROUP_ID,
            "auto.offset.reset": "earliest",
        }
    )
    consumer.subscribe([KAFKA_TOPIC])
    logging.info(f"Worker {worker_id} subscribed to Kafka topic '{KAFKA_TOPIC}'.")
    batch = []
    last_insert_time = datetime.now()
    insert_query = f"INSERT INTO {CLICKHOUSE_DB}.{CLICKHOUSE_TABLE} (trade_id, source, symbol, side, price, base_amount, quote_amount, event_time) VALUES"
    try:
        while not stop_event.is_set():
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                if (
                    batch
                    and (datetime.now() - last_insert_time).total_seconds()
                    > BATCH_TIMEOUT_SECONDS
                ):
                    client.execute(insert_query, batch)
                    logging.info(
                        f"Worker {worker_id}: Inserted a partial batch of {len(batch)} records due to timeout."
                    )
                    batch.clear()
                    last_insert_time = datetime.now()
                continue
            if msg.error():
                raise KafkaException(msg.error())
            if msg:
                transformed_trades = parse_and_transform(msg.value().decode("utf-8"))
                if transformed_trades:
                    batch.extend(transformed_trades)
            if len(batch) >= BATCH_SIZE:
                client.execute(insert_query, batch)
                logging.info(
                    f"Worker {worker_id}: Inserted a full batch of {len(batch)} records."
                )
                batch.clear()
                last_insert_time = datetime.now()
    except KafkaException as e:
        logging.error(f"Worker {worker_id}: Kafka error: {e}")
    except Exception as e:
        logging.error(
            f"Worker {worker_id}: An unexpected error occurred: {e}", exc_info=True
        )
    finally:
        if batch:
            try:
                client.execute(insert_query, batch)
                logging.info(
                    f"Worker {worker_id}: Inserted final batch of {len(batch)} records before shutdown."
                )
            except Exception as e:
                logging.error(f"Worker {worker_id}: Failed to insert final batch: {e}")
        consumer.close()
        logging.info(f"Worker {worker_id}: Kafka consumer closed.")


def main(num_workers=1):
    client = get_clickhouse_client()
    if not client:
        return
    setup_database(client)
    logging.info(f"Starting {num_workers} workers for topic '{KAFKA_TOPIC}'.")
    stop_event = threading.Event()
    threads = []
    for i in range(num_workers):
        t = threading.Thread(target=consumer_worker, args=(i, stop_event))
        t.start()
        threads.append(t)
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logging.info("Shutting down consumers.")
        stop_event.set()
    except Exception as e:
        logging.error(f"Main: An unexpected error occurred: {e}", exc_info=True)
        stop_event.set()
    for t in threads:
        t.join()
    logging.info("All workers stopped.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-r",
        "--drop_database",
        help="Recreate Database",
        action=argparse.BooleanOptionalAction,
    )
    parser.add_argument(
        "--num-workers", type=int, default=1, help="Number of consumer workers to run"
    )
    args = parser.parse_args()
    if args.drop_database:
        restart_database()
    else:
        main(args.num_workers)
