from kafka import KafkaProducer 
import json

# Kafka Producer set-up
producer = KafkaProducer(value_serializer=lambda m: json.dumps(m).encode('utf-8'),
                         bootstrap_servers=['localhost:9092'])

for i in range(20):
    message = {'id': i, 'message': f'Message number: {i}'}

    producer.send('rtppmdata.nationalpage.nationalppm', value=message)

producer.flush()
producer.close()
