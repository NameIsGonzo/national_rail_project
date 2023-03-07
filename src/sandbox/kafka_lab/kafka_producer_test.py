from kafka import KafkaProducer 
import json

# Kafka Producer set-up
producer = KafkaProducer(bootstrap_servers=['localhost:9092'])

# for i in range(20):
with open('/Users/gonzo/Desktop/RailScope/national_rail_project/src/sandbox/stomp/rtppm/rtppm_data_copy.json', 'r') as f:
    data = json.load(f)
    message = json.dumps(data)

producer.send('test_topic_rtppm', value=message.encode('utf-8'))

producer.close()