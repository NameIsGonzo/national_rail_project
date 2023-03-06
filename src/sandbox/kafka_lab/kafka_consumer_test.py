from kafka import KafkaConsumer

consumer = KafkaConsumer('test_topic_rtppm',
                         bootstrap_servers=['localhost:9092'],
                         group_id='1',
                         auto_offset_reset='earliest')


for message in consumer:
    print(message.value.decode('utf-8'))

consumer.close()