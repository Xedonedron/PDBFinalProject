from kafka import KafkaConsumer
from pyhdfs import HdfsClient

consumer = KafkaConsumer('twitter', bootstrap_servers=['localhost:9092'])
hdfs_host = 'hdfs://localhost'
hdfs_port = 9000
hdfs_path = '/PDB/twitter/tweets.txt'

client = HdfsClient(hosts=f'{hdfs_host}:{hdfs_port}')

with client.open(hdfs_path, mode='at') as f:
    for message in consumer:
        values = message.value.decode('utf-8')
        f.write(f'{values}\n')
        print(message.value)
