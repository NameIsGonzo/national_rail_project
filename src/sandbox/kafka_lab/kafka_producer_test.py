from kafka import KafkaProducer 


# Kafka Producer set-up
producer = KafkaProducer(bootstrap_servers=['localhost:9092'])

for i in range(20):
    message: str = '{"id": %s, "name": "John"}' % str(i)
    producer.send('test_topic_rtppm', value=message.encode('utf-8'))

producer.close()