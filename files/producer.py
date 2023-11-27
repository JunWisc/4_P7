from kafka import KafkaProducer
from kafka.errors import KafkaError
import weather
import report_pb2

# Set up Kafka producer
producer = KafkaProducer(
         bootstrap_servers=['localhost:9092'],
         retries=10,
         acks='all',
         key_serializer=str.encode,
         value_serializer=lambda m: m.SerializeToString()
    )

# Runs infinitely because the weather never ends
for date, degrees in weather.get_next_weather(delay_sec=0.1):
    # Create Protobuf message
    report = report_pb2.Report(date=date, degrees=degrees)

    # Generate the key for the message using the month name
    month_name = date.split('-')[1]
    month_names = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December']
    key = month_names[int(month_name) - 1]

    # Send the message to Kafka
    future = producer.send('temperatures', key=key, value=report)

    # Handle sending errors
    try:
        future.get(timeout=10)
    except KafkaError as e:
        print(f"Failed to send message: {e}")
