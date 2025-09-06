import json
from kafka.client import KafkaClient



client = KafkaClient(group_id='test')

topic = 'test_data'
data = [
    {
        'id': 1,
     'name': 'navid'
     }
     for _ in range(10)
]

try:
    for i, d in enumerate(data):
        key = f'crawl_test_{i}'
        value = json.dumps(d)
        data = {
            key: value
        }
        client.start_sending_message(data, topic=topic)

    data = client.start_consume(topic)

    print(data)
except:
    pass

