from confluent_kafka import Producer, Consumer

class KafkaClient:
    def __init__(self, group_id: str) -> None:
        self.conf_producer = {
            'bootstrap.servers': 'localhost:9092'
        } 
        self.conf_consumer = {
            **self.conf_producer,
            'group.id': group_id,
            'auto.offset.reset': 'earliest'

        }
        self.cosumer_client = self.create_consume_client()
        self.produce_client = self.create_produce_client()

    def create_consume_client(self):
        return Consumer(
            **self.conf_consumer
        )
    
    def create_produce_client(self):
        return Producer(**self.conf_producer)

    @staticmethod
    def delivery_report(err, msg):
        """ Called once for each message produced to indicate delivery result. """
        if err is not None:
            print(f"Message delivery failed: {err}")
        else:
            print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

    def send_message(self, producer: Producer, key, value, topic, delivery_report=None):
        if not delivery_report:
            delivery_report = self.delivery_report

        producer.produce(
                topic, 
                key=key.encode('utf-8'), 
                value=value.encode('utf-8'), 
                callback=delivery_report
            )
    
    def start_sending_message(self, data, topic):
        producer = self.produce_client
        for d in data:
            self.send_message(
                producer,
                key=d,
                value=data[d],
                topic=topic
            )
        producer.flush()

    def consume_data(self, consumer: Consumer, topic):
        data = []
        consumer.subscribe([topic])
        try:
            while True:
                msg = consumer.poll(1.0)
                if msg is None:
                    continue
                if msg.error():
                    print(f"Consumer error: {msg.error()}")
                    continue
                data.append({
                    '{}'.format(msg.key().decode('utf-8')): msg.value().decode('utf-8')
                })
        except:
            consumer.close()
        return data
            
    
    def start_consume(self, topic):
        return self.consume_data(self.cosumer_client, topic=topic)
