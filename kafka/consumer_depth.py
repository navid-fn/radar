import logging
import os
import json
import argparse
from datetime import datetime

from clickhouse_driver import Client
from confluent_kafka import Consumer, KafkaException
from dotenv import load_dotenv

load_dotenv()

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "radar_depths")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "clickhouse-consumers-depth-v4")

CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "localhost")
CLICKHOUSE_PORT = os.getenv("CLICKHOUSE_PORT", "9000")
CLICKHOUSE_DB = os.getenv("CLICKHOUSE_DB", "trades")
CLICKHOUSE_TABLE = os.getenv("CLICKHOUSE_TABLE", "orderbook_depth")
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "default")

LOG_LEVEL = getattr(logging, os.getenv("LOG_LEVEL", "INFO").upper(), logging.INFO)
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "500"))  # Increased for better performance
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
    logging.info(f"Dropped database '{CLICKHOUSE_DB}'")


def setup_database(client):
    """
    Creates the database and optimized orderbook depth table.
    
    OPTION 1: Array-based (RECOMMENDED for throttled snapshots)
    - One row per symbol per snapshot
    - Stores all price levels as arrays
    """
    try:
        logging.info(f"Creating database '{CLICKHOUSE_DB}' if not exists...")
        client.execute(f"CREATE DATABASE IF NOT EXISTS {CLICKHOUSE_DB}")

        logging.info(f"Creating table '{CLICKHOUSE_TABLE}' with optimized schema...")
        
        # Array-based schema (best for your throttled approach)
        client.execute(
            f"""
        CREATE TABLE IF NOT EXISTS {CLICKHOUSE_DB}.{CLICKHOUSE_TABLE} (
            timestamp DateTime64(3),                    -- Event time with millisecond precision
            exchange LowCardinality(String),            -- 'wallex'
            symbol LowCardinality(String),              -- Trading pair
            side LowCardinality(String),                -- 'sell' or 'buy'
            
            -- Arrays for price levels (ordered best to worst)
            prices Array(Float64),                      -- Price levels
            quantities Array(Float64),                  -- Quantities at each level
            sums Array(Float64),                        -- Cumulative sums (if available)
            
            -- Metadata
            ingestion_time DateTime DEFAULT now(),      -- When inserted into ClickHouse
            levels UInt8 DEFAULT length(prices)         -- Number of levels in snapshot
            
        ) ENGINE = MergeTree()
        PARTITION BY toYYYYMMDD(timestamp)              -- Daily partitions
        ORDER BY (symbol, side, timestamp)              -- Query optimization
        TTL toDateTime(timestamp) + INTERVAL 90 DAY     -- Auto-delete after 90 days (cast to DateTime)
        SETTINGS index_granularity = 8192
        """
        )
        
        logging.info("Database and optimized orderbook table setup complete.")
    except Exception as e:
        logging.error(f"Error during database setup: {e}")
        raise


def setup_database_flat(client):
    """
    OPTION 2: Flat schema (if you prefer normalized structure)
    - One row per price level
    - More rows but easier to query individual levels
    """
    try:
        logging.info(f"Creating database '{CLICKHOUSE_DB}' if not exists...")
        client.execute(f"CREATE DATABASE IF NOT EXISTS {CLICKHOUSE_DB}")

        logging.info(f"Creating table '{CLICKHOUSE_TABLE}' with flat schema...")
        
        client.execute(
            f"""
        CREATE TABLE IF NOT EXISTS {CLICKHOUSE_DB}.{CLICKHOUSE_TABLE} (
            timestamp DateTime64(3),                    -- Event time
            exchange LowCardinality(String),            -- 'wallex'
            symbol LowCardinality(String),              -- Trading pair
            side LowCardinality(String),                -- 'sell' or 'buy'
            
            level UInt8,                                -- 0 = best price, 1 = second best, etc.
            price Float64,                              -- Price at this level
            quantity Float64,                           -- Quantity at this level
            sum Float64,                                -- Cumulative sum (if available)
            
            ingestion_time DateTime DEFAULT now()
            
        ) ENGINE = MergeTree()
        PARTITION BY toYYYYMMDD(timestamp)
        ORDER BY (symbol, side, timestamp, level)
        TTL toDateTime(timestamp) + INTERVAL 90 DAY     -- Cast to DateTime for TTL
        SETTINGS index_granularity = 8192
        """
        )
        
        logging.info("Database and flat orderbook table setup complete.")
    except Exception as e:
        logging.error(f"Error during database setup: {e}")
        raise


def _transform_wallex_depths_array(data):
    """
    Transforms depth message into array format (one row per snapshot).
    
    Input:
    [
        "USDTTMN@sellDepth",
        [
            { "quantity": 255.75, "price": 82131, "sum": 21005003.25 },
            { "quantity": 103.07, "price": 82083, "sum": 8460294.81 },
            ...
        ]
    ]
    
    Output: Single tuple with arrays
    """
    try:
        channel, side_info = data[0].split('@')
        depth_data = data[1]
        
        # Determine side from channel name
        if 'sell' in side_info.lower():
            side = 'sell'
        elif 'buy' in side_info.lower():
            side = 'buy'
        else:
            side = 'unknown'
        
        # Extract arrays
        prices = [d['price'] for d in depth_data]
        quantities = [d['quantity'] for d in depth_data]
        sums = [d.get('sum', 0.0) for d in depth_data]
        
        # Use current time as timestamp (since Wallex doesn't provide one in depth messages)
        # Alternatively, you could add timestamp in your Go producer
        timestamp = datetime.now()
        
        row = (
            timestamp,      # timestamp
            'wallex',       # exchange
            channel,        # symbol
            side,           # side
            prices,         # prices array
            quantities,     # quantities array
            sums,           # sums array
        )
        
        return [row]
        
    except Exception as e:
        logging.error(f"Failed to transform Wallex depth (array). Error: {e}. Data: {data}")
        return []


def _transform_wallex_depths_flat(data):
    """
    Transforms depth message into flat format (one row per level).
    
    Same input as above, but outputs multiple rows.
    """
    try:
        channel, side_info = data[0].split('@')
        depth_data = data[1]
        
        # Determine side
        if 'sell' in side_info.lower():
            side = 'sell'
        elif 'buy' in side_info.lower():
            side = 'buy'
        else:
            side = 'unknown'
        
        timestamp = datetime.now()
        transformed_data = []
        
        for level, d in enumerate(depth_data):
            row = (
                timestamp,          # timestamp
                'wallex',           # exchange
                channel,            # symbol
                side,               # side
                level,              # level (0 = best price)
                d['price'],         # price
                d['quantity'],      # quantity
                d.get('sum', 0.0),  # sum
            )
            transformed_data.append(row)
        
        return transformed_data
        
    except Exception as e:
        logging.error(f"Failed to transform Wallex depth (flat). Error: {e}. Data: {data}")
        return []


def parse_and_transform(message_value, use_array_format=True):
    """
    Inspects the message format and routes it to the correct transformation function.
    
    Args:
        message_value: Raw message from Kafka
        use_array_format: If True, use array format; if False, use flat format
    """
    try:
        data = json.loads(message_value)
        
        # Skip ping messages
        if isinstance(data, dict) and data.get('pingInterval'):
            return []
        
        # Choose transformation based on format
        if use_array_format:
            return _transform_wallex_depths_array(data)
        else:
            return _transform_wallex_depths_flat(data)
            
    except Exception as e:
        logging.error(f"Failed to parse JSON. Error: {e}.")
        return []


def main(use_array_format=True):
    """
    Main function to consume from Kafka and insert into ClickHouse.
    
    Args:
        use_array_format: If True, use array-based schema; if False, use flat schema
    """
    client = get_clickhouse_client()
    if not client:
        return

    # Setup database with chosen schema
    if use_array_format:
        setup_database(client)
        insert_query = f"""
            INSERT INTO {CLICKHOUSE_DB}.{CLICKHOUSE_TABLE} 
            (timestamp, exchange, symbol, side, prices, quantities, sums) 
            VALUES
        """
    else:
        setup_database_flat(client)
        insert_query = f"""
            INSERT INTO {CLICKHOUSE_DB}.{CLICKHOUSE_TABLE} 
            (timestamp, exchange, symbol, side, level, price, quantity, sum) 
            VALUES
        """

    consumer_conf = {
        "bootstrap.servers": KAFKA_BROKER,
        "group.id": KAFKA_GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
        "auto.commit.interval.ms": 5000,
    }
    consumer = Consumer(consumer_conf)
    consumer.subscribe([KAFKA_TOPIC])
    logging.info(f"Subscribed to Kafka topic '{KAFKA_TOPIC}'.")
    logging.info(f"Using {'ARRAY' if use_array_format else 'FLAT'} schema format.")

    batch = []
    last_insert_time = datetime.now()
    message_count = 0

    try:
        while True:
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                # Timeout - check if we should flush batch
                if (
                    batch
                    and (datetime.now() - last_insert_time).total_seconds()
                    > BATCH_TIMEOUT_SECONDS
                ):
                    client.execute(insert_query, batch)
                    logging.info(
                        f"Inserted partial batch of {len(batch)} records due to timeout."
                    )
                    batch.clear()
                    last_insert_time = datetime.now()
                continue

            if msg.error():
                raise KafkaException(msg.error())

            if msg:
                transformed_depths = parse_and_transform(
                    msg.value().decode("utf-8"), 
                    use_array_format=use_array_format
                )
                if transformed_depths:
                    batch.extend(transformed_depths)
                    message_count += 1

            # Insert when batch is full
            if len(batch) >= BATCH_SIZE:
                client.execute(insert_query, batch)
                logging.info(
                    f"Inserted full batch of {len(batch)} records "
                    f"({message_count} messages processed)."
                )
                batch.clear()
                last_insert_time = datetime.now()
                message_count = 0

    except KeyboardInterrupt:
        logging.info("Shutting down consumer.")
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}", exc_info=True)
    finally:
        # Insert remaining batch
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
    parser = argparse.ArgumentParser(
        description="Consume orderbook depth data from Kafka and insert into ClickHouse"
    )
    parser.add_argument(
        "-r",
        "--drop_database",
        help="Drop and recreate database",
        action="store_true",
    )
    parser.add_argument(
        "--format",
        choices=["array", "flat"],
        default="array",
        help="Schema format: 'array' (recommended) or 'flat' (default: array)",
    )
    args = parser.parse_args()
    
    if args.drop_database:
        restart_database()
    else:
        use_array = (args.format == "array")
        main(use_array_format=use_array)
