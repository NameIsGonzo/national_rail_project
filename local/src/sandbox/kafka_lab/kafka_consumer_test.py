from kafka import KafkaConsumer
from multiprocessing import Process

def consume(topic):
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=['localhost:9092'],
        group_id='1',
        auto_offset_reset='earliest'
    )
    for message in consumer:
        print(f'{topic}: {message.value.decode("utf-8")}')

if __name__ == '__main__':
    topics = [
        'rtppmdata.nationalpage.nationalppm',
        'rtppmdata.nationalpage.sector',
        'rtppmdata.nationalpage.operator',
        'rtppmdata.oocpage.operator',
        # 'rtppmdata.focpage.nationalppm',
        # 'rtppmdata.focpage.operator',
        # 'rtppmdata.operatorpage.operators',
        # 'rtppmdata.operatorpage.servicegroups'
    ]
    processes = []
    for topic in topics:
        p = Process(target=consume, args=(topic,))
        processes.append(p)
        p.start()
    for p in processes:
        p.join()
