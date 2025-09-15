import asyncio
import json
import logging
import os
from dotenv import load_dotenv

import requests
import websockets
from confluent_kafka import Producer

load_dotenv()

BITPIN_API_URL = "https://api.bitpin.ir/api/v1/mkt/markets/"
BITPIN_WS_URL = "wss://ws.bitpin.ir"
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
KAFKA_TOPIC = "radar_trades"
LOG_LEVEL = logging.INFO
MAX_SUBS_PER_CONNECTION = 40
PING_INTERVAL_SECONDS = 20
PING_TIMEOUT_SECONDS = 20

# --- Setup Logging ---
logging.basicConfig(level=LOG_LEVEL, format='%(asctime)s - %(levelname)s - %(message)s')


def get_kafka_producer():
    """Initializes and returns a Kafka Producer instance."""
    try:
        producer = Producer({'bootstrap.servers': KAFKA_BROKER})
        logging.info("Kafka Producer initialized successfully.")
        return producer
    except Exception as e:
        logging.error(f"Failed to initialize Kafka Producer: {e}")
        return None


def get_markets():
    """Fetches all trading markets from BitPin."""
    try:
        response = requests.get(BITPIN_API_URL)
        response.raise_for_status()
        data = response.json()
        markets = sorted([i['symbol'] for i in data if i['tradable']])
        logging.info(f"Fetched {len(markets)} unique markets from Bitpin API.")
        return markets
    except requests.RequestException as e:
        logging.error(f"Error fetching markets from Bitpin API: {e}")
        return []
    except (KeyError, TypeError) as e:
        logging.error(f"Unexpected API response format from Bitpin: {e}")
        return []

def delivery_report(err, msg):
    """Callback for Kafka message delivery."""
    if err is not None:
        logging.error(f'Message delivery failed: {err}')

async def websocket_worker(symbols_chunk, producer):
    """
    A worker that manages a WebSocket connection for a chunk of symbols,
    with automatic ping-pong handling.
    """
    worker_id = f"Worker-{symbols_chunk[0]}"
    logging.info(f"[{worker_id}] Starting for {len(symbols_chunk)} symbols.")
    
    while True:
        try:
            async with websockets.connect(
                BITPIN_WS_URL, 
                ping_interval=PING_INTERVAL_SECONDS,
                ping_timeout=PING_TIMEOUT_SECONDS
            ) as websocket:
                logging.info(f"[{worker_id}] Connected to WebSocket.")
                
                logging.info(f"[{worker_id}] Subscribing to {len(symbols_chunk)} markets...")
                subscription_msg = json.dumps({"method":"sub_to_market_data", "symbols":symbols_chunk})
                await websocket.send(subscription_msg)
                
                logging.info(f"[{worker_id}] Subscriptions sent.")

                async for message in websocket:
                    if '"message":"PONG"' in message:
                        logging.info(f"[{worker_id}] < PONG received")
                        continue
                    
                    producer.poll(0)
                    producer.produce(KAFKA_TOPIC, message.encode('utf-8'), callback=delivery_report)

        except (websockets.ConnectionClosed, websockets.InvalidURI, asyncio.TimeoutError) as e:
            logging.error(f"[{worker_id}] WebSocket error: {e}. Reconnecting in 5s...")
        except Exception as e:
            logging.critical(f"[{worker_id}] An unexpected error occurred: {e}. Reconnecting in 5s...")
        finally:
            await asyncio.sleep(5)

async def main():
    """Main function to orchestrate all websocket workers."""
    logging.info("Starting Bitpin data producer orchestrator...")
    kafka_producer = get_kafka_producer()
    if not kafka_producer:
        logging.critical("Exiting due to Kafka producer initialization failure.")
        return
    
    all_markets = get_markets()
    if not all_markets:
        logging.critical("Could not fetch any markets to subscribe to. Shutting down.")
        return

    # Chunk the markets into smaller lists
    market_chunks = [
        all_markets[i:i + MAX_SUBS_PER_CONNECTION] 
        for i in range(0, len(all_markets), MAX_SUBS_PER_CONNECTION)
    ]
    logging.info(f"Divided {len(all_markets)} markets into {len(market_chunks)} chunks of ~{MAX_SUBS_PER_CONNECTION}.")

    # Create and run a worker task for each chunk
    tasks = [
        asyncio.create_task(websocket_worker(chunk, kafka_producer))
        for chunk in market_chunks
    ]
    
    await asyncio.gather(*tasks)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("Producer stopped by user.")

