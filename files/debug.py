
from kafka import KafkaConsumer
import report_pb2
import json

consumer = KafkaConsumer(
        'temperatures',
        bootstrap_servers=['localhost:9092'],
        group_id='debug',
        value_deserializer=lambda m: report_pb2.Report().FromString(m)
     )

for message in consumer:
     report = report_pb2.Report()
     report.ParseFromString(message.value)
     print(json.dumps({
         'partition': message.partition,
         'key': message.key.decode('utf-8'),
         'date': report.date,
         'degrees': report.degrees
         }))
     
