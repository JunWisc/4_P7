from kafka import KafkaAdminClient, KafkaProducer
from kafka.admin import NewTopic
from kafka.errors import KafkaError, UnknownTopicOrPartitionError
import weather
import report_pb2
from datetime import datetime

broker = 'localhost:9092'
admin_client = KafkaAdminClient(bootstrap_servers=[broker])

try:
    admin_client.delete_topics(["temperatures"])
    print("Deleted topics successfully")
except UnknownTopicOrPartitionError:
    print("Cannot delete topic/s (may not exist yet)")

new_topic = NewTopic(name="temperatures", num_partitions=4, replication_factor=1)
admin_client.create_topics(new_topics=[new_topic])

print("Topics:", admin_client.list_topics())

producer = KafkaProducer(bootstrap_servers=[broker],
                         retries=10,
                         acks='all')

for date, degrees in weather.get_next_weather():
    report = report_pb2.Report(date=date, degrees=degrees)
    month = datetime.strptime(date, "%Y-%m-%d").strftime("%B")
    future = producer.send('temperatures', key=month.encode(), value=report.SerializeToString())

    try:
        record_metadata = future.get(timeout=10)
    except KafkaError:
        pass

producer.flush()
producer.close()
