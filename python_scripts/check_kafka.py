import time
from confluent_kafka import Consumer, TopicPartition, KafkaException
from tabulate import tabulate

# --- CONFIGURATION ---
BROKER = "localhost:9092"  # Change to "kafka:9092" if inside Docker
TOPICS = ["radar_orderbook", "radar_candle", "radar_trades"]
SAMPLE_INTERVAL = 60 * 5

def get_total_offsets(consumer, topics):
    """Fetches high-watermark offsets."""
    total_offsets = {}
    try:
        metadata = consumer.list_topics(timeout=10)
    except KafkaException as e:
        print(f"Error connecting: {e}")
        return {}

    for topic in topics:
        if topic not in metadata.topics:
            continue
        
        partitions = metadata.topics[topic].partitions
        topic_total = 0
        for p_id in partitions:
            tp = TopicPartition(topic, p_id)
            try:
                _, high = consumer.get_watermark_offsets(tp, timeout=2.0)
                topic_total += high
            except Exception:
                pass
        total_offsets[topic] = topic_total
    return total_offsets

def get_avg_message_size(consumer, topic, samples=10):
    """Consumes messages to estimate size. Uses 'earliest' to ensure data is found."""
    temp_conf = {
        'bootstrap.servers': BROKER,
        'group.id': 'stats-estimator-temp-' + str(time.time()),
        'auto.offset.reset': 'earliest',  # CHANGED: Grab existing data immediately
        'enable.auto.commit': False
    }
    temp_consumer = Consumer(temp_conf)
    temp_consumer.subscribe([topic])
    
    total_bytes = 0
    count = 0
    start = time.time()
    
    # Increased timeout to 5 seconds to allow connection time
    while count < samples and (time.time() - start) < 5:
        msg = temp_consumer.poll(1.0)
        if msg is None or msg.error():
            continue
        total_bytes += len(msg.value())
        count += 1
    
    temp_consumer.close()
    return total_bytes / count if count > 0 else 0

def main():
    conf = {'bootstrap.servers': BROKER, 'group.id': 'monitor-main', 'auto.offset.reset': 'latest'}
    consumer = Consumer(conf)
    
    print(f"--- Kafka Monitor Started on {BROKER} ---")
    
    prev_offsets = get_total_offsets(consumer, TOPICS)
    avg_sizes = {}

    try:
        while True:
            time.sleep(SAMPLE_INTERVAL)
            
            # 1. Update Offsets (Rate)
            curr_offsets = get_total_offsets(consumer, TOPICS)
            
            report_data = []
            
            for topic in TOPICS:
                if topic not in curr_offsets or topic not in prev_offsets:
                    continue
                
                # 2. Lazy Load Message Size (Retry if 0)
                if avg_sizes.get(topic, 0) == 0:
                    avg_sizes[topic] = get_avg_message_size(consumer, topic)

                # Calculations
                delta_msgs = curr_offsets[topic] - prev_offsets[topic]
                rate_mps = delta_msgs / SAMPLE_INTERVAL
                
                avg_size = avg_sizes.get(topic, 0)
                throughput_kb = (rate_mps * avg_size) / 1024
                daily_gb = (throughput_kb * 86400) / (1024 * 1024)

                report_data.append([
                    topic, 
                    f"{rate_mps:.1f}", 
                    f"{throughput_kb:.2f} KB/s", 
                    f"{int(avg_size)} B",
                    f"{daily_gb:.2f} GB"
                ])
            
            print("\n" + "="*65)
            print(tabulate(report_data, headers=["Topic", "Msgs/Sec", "Throughput", "Avg Size", "Est. Daily Storage"]))
            
            prev_offsets = curr_offsets

    except KeyboardInterrupt:
        print("\nStopping.")
    finally:
        consumer.close()

if __name__ == "__main__":
    main()
