from kafka import KafkaProducer
import pandas as pd
from json import dumps
import time

KAFKA_TOPIC_NAME = 'customer_demographics'
KAFKA_BOOTSTRAP_SERVERS = 'kafka:9092'

def generate_customer_demographics_data(filename):
    """
    Streams customer demographics data from a CSV file to Kafka.
    """
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda x: dumps(x).encode('utf-8')
    )
    data = pd.read_csv(filename).to_dict(orient='records')
    for record in data:
        producer.send(KAFKA_TOPIC_NAME, record)
        time.sleep(1)  # Simulate real-time streaming