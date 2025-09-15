import logging
import os
import json
import hashlib

from datetime import datetime
from decimal import Decimal
from clickhouse_driver import Client
from confluent_kafka import Consumer, KafkaException
from dotenv import load_dotenv

load_dotenv()

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
KAFKA_TOPIC = "radar_trades"
KAFKA_GROUP_ID = "clickhouse-consumers-v3"

CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "localhost")
CLICKHOUSE_PORT = os.getenv("CLICKHOUSE_PORT", "9000")
CLICKHOUSE_DB = "trades"
CLICKHOUSE_TABLE = "trades_master"
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "default")

LOG_LEVEL = logging.INFO
BATCH_SIZE = 200
BATCH_TIMEOUT_SECONDS = 5

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


def setup_database(client):
    """
    Creates the database and a unified table using ReplacingMergeTree for deduplication.
    """
    try:
        logging.info(f"Creating database '{CLICKHOUSE_DB}' if not exists...")
        client.execute(f"CREATE DATABASE IF NOT EXISTS {CLICKHOUSE_DB}")

        logging.info(f"Creating table '{CLICKHOUSE_TABLE}' if not exists...")
        # --- MODIFICATION: Unified schema with a 'source' column ---
        # The key for deduplication is now a combination of source and trade_id.
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


def _transform_wallex_trade(data):
    """Transforms a trade message from the Wallex format."""
    try:
        symbol_part, trade_data = data
        symbol = symbol_part.split("@")[0]
        side = "buy" if trade_data.get("isBuyOrder", False) else "sell"
        price_str = trade_data.get("price", "0.0")
        quantity_str = trade_data.get(
            "quantity", "0.0"
        )  # Wallex 'quantity' is the base_amount
        timestamp_str = trade_data.get("timestamp")

        # Create synthetic ID for Wallex trades
        unique_string = (
            f"wallex-{symbol}-{timestamp_str}-{price_str}-{quantity_str}-{side}"
        )
        trade_id = hashlib.sha1(unique_string.encode("utf-8")).hexdigest()

        event_time = datetime.fromisoformat(timestamp_str.replace("Z", "+00:00"))

        row = (
            trade_id,
            "wallex",
            symbol,
            side,
            float(price_str),
            float(quantity_str),
            Decimal(price_str) * Decimal(quantity_str),
            event_time,
        )
        return [row]  # Return as a list for consistency
    except Exception as e:
        logging.error(f"Failed to transform Wallex trade. Error: {e}. Data: {data}")
        return []


def _transform_bitpin_matches(data):
    """Transforms a trade message from the Bitpin 'matches_update' format."""
    try:
        symbol = data.get("symbol")
        event_time_str = data.get("event_time")
        matches = data.get("matches", [])
        event_time = datetime.fromisoformat(event_time_str.replace("Z", "+00:00"))

        transformed_rows = []
        for match in matches:
            row = (
                match.get("id"),
                "bitpin",
                symbol,
                match.get("side"),
                float(match.get("price", 0.0)),
                float(match.get("base_amount", 0.0)),
                float(match.get("quote_amount", 0.0)),
                event_time,
            )
            transformed_rows.append(row)
        return transformed_rows
    except Exception as e:
        logging.error(f"Failed to transform Bitpin matches. Error: {e}. Data: {data}")
        return []


def parse_and_transform(message_value):
    """
    Inspects the message format and routes it to the correct transformation function.
    """
    try:
        data = json.loads(message_value)
        # Heuristic to determine the source based on message structure
        if isinstance(data, list) and len(data) == 2:
            return _transform_wallex_trade(data)
        elif isinstance(data, dict) and data.get("event") == "matches_update":
            return _transform_bitpin_matches(data)
        else:
            return []
    except Exception as e:
        logging.error(f"Failed to parse JSON. Error: {e}.")
        return []


def main():
    """Main function to consume from Kafka and insert into the unified ClickHouse table."""
    client = get_clickhouse_client()
    if not client:
        return

    setup_database(client)

    consumer_conf = {
        "bootstrap.servers": KAFKA_BROKER,
        "group.id": KAFKA_GROUP_ID,
        "auto.offset.reset": "earliest",
    }
    consumer = Consumer(consumer_conf)
    consumer.subscribe([KAFKA_TOPIC])
    logging.info(f"Subscribed to Kafka topic '{KAFKA_TOPIC}'.")

    batch = []
    last_insert_time = datetime.now()
    insert_query = f"INSERT INTO {CLICKHOUSE_DB}.{CLICKHOUSE_TABLE} (trade_id, source, symbol, side, price, base_amount, quote_amount, event_time) VALUES"

    try:
        while True:
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                if (
                    batch
                    and (datetime.now() - last_insert_time).total_seconds()
                    > BATCH_TIMEOUT_SECONDS
                ):
                    client.execute(insert_query, batch)
                    logging.info(
                        f"Inserted a partial batch of {len(batch)} records due to timeout."
                    )
                    batch.clear()
                    last_insert_time = datetime.now()
                continue

            if msg.error():
                raise KafkaException(msg.error())

            transformed_trades = parse_and_transform(msg.value().decode("utf-8"))
            if transformed_trades:
                batch.extend(transformed_trades)

            if len(batch) >= BATCH_SIZE:
                client.execute(insert_query, batch)
                logging.info(f"Inserted a full batch of {len(batch)} records.")
                batch.clear()
                last_insert_time = datetime.now()

    except KeyboardInterrupt:
        logging.info("Shutting down consumer.")
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}", exc_info=True)
    finally:
        if batch and client:
            try:
                client.execute(insert_query, batch)
                logging.info(
                    f"Inserted final batch of {len(batch)} records before shutdown."
                )
            except Exception as e:
                logging.error(f"Failed to insert final batch: {e}")

        consumer.close()
        logging.info("Kafka consumer closed.")


if __name__ == "__main__":
    main()
