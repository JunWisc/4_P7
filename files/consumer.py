import os
import json
import time
from kafka import KafkaConsumer, TopicPartition
import report_pb2
import sys

def update_stats(stats, report):
    if report.date < stats['start']:
        stats['start'] = report.date
    if report.date > stats['end']:
        stats['end'] = report.date
    stats['count'] += 1
    stats['sum'] += report.degrees
    stats['avg'] = stats['sum'] / stats['count']

def load_partition_data(partition):
    path = f"partition-{partition}.json"
    if os.path.exists(path):
        with open(path, 'r') as f:
            data = json.load(f)
    else:
        data = {"partition": partition, "offset": 0}
    return data

def save_partition_data(partition, data):
    path = f"partition-{partition}.json"
    path_tmp = path + ".tmp"
    with open(path_tmp, "w") as f:
        json.dump(data, f)
    os.rename(path_tmp, path)

def main():
    partitions = [int(p) for p in sys.argv[1:]]
    topic_partitions = [TopicPartition('temperatures', p) for p in partitions]
    
    consumer = KafkaConsumer(bootstrap_servers=['localhost:9092'],
                             value_deserializer=lambda m: report_pb2.Report().FromString(m))

    consumer.assign(topic_partitions)
    
    partition_data = {p: load_partition_data(p) for p in partitions}

    for tp in topic_partitions:
        consumer.seek(tp, partition_data[tp.partition]['offset'])

    for message in consumer:
        report = message.value
        data = partition_data[message.partition]
        month = message.key.decode()
        year = report.date.split('-')[0]

        if month not in data:
            data[month] = {}
        if year not in data[month]:
            data[month][year] = {"count": 0, "sum": 0, "avg": 0, "start": report.date, "end": report.date}

        update_stats(data[month][year], report)
        data['offset'] = message.offset + 1
        save_partition_data(message.partition, data)

if __name__ == "__main__":
        main()


