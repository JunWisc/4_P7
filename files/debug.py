from kafka import KafkaConsumer
import report_pb2

consumer = KafkaConsumer('temperatures',
                          group_id='debug',
                          bootstrap_servers=['localhost:9092'],
                          value_deserializer=lambda m: report_pb2.Report().FromString(m))

for message in consumer:
    print({'partition': message.partition, 'key': message.key.decode(), 'date': message.value.date, 'degrees': message.value.degrees})
