from kafka.client import KafkaClient



client = KafkaClient(group_id='test')

topic = 'test_data'
data = [
    'crawl_data'
     for _ in range(10)
]

for i, d in enumerate(data):
    key = f'crawl_test_{i}'
    data = {
        key: d
    }
    client.start_sending_message(data, topic=topic)

data = client.start_consume(topic)

print(data)
