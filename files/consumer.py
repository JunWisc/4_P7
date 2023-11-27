from kafka import KafkaConsumer, TopicPartition
import report_pb2
import json
import os
import sys

partitions = [int(arg) for arg in sys.argv[1:]]

consumer = KafkaConsumer(
        bootstrap_servers=['localhost:9092'],
        value_deserializer=lambda m: report_pb2.Report().FromString(m)
    )
consumer.assign([TopicPartition('temperatures', p) for p in partitions])

partition_data = {}

for p in partitions:
    filename = f'partition-{p}.json'
    if os.path.exists(filename):
        with open(filename, 'r') as f:
            partition_data[p] = json.load(f)
        consumer.seek(TopicPartition('temperatures', p), partition_data[p]['offset'])
    else:
         partition_data[p] = {'partition': p, 'offset': 0}

for message in consumer:
    report = report_pb2.Report()
    report.ParseFromString(message.value)
    month = message.key.decode('utf-8')
    year = report.date.split('-')[0]

    if month not in partition_data[message.partition]:
        partition_data[message.partition][month] = {}

    if year not in partition_data[message.partition][month]:
        partition_data[message.partition][month][year] = {
                'count': 0,
                'sum': 0,
                'avg': 0,
                'start': report.date,
                'end': report.date
        }

    data = partition_data[message.partition][month][year]

    if report.date <= data['end']:
        continue  # Ignore duplicate dates
    data['count'] += 1
    data['sum'] += report.degrees
    data['avg'] = data['sum'] / data['count']
    data['end'] = report.date
    partition_data[message.partition]['offset'] = message.offset

    with open(f'partition-{message.partition}.json.tmp', 'w') as f:
        json.dump(partition_data[message.partition], f)
    os.rename(f'partition-{message.partition}.json.tmp', f'partition-{message.partition}.json')
   
