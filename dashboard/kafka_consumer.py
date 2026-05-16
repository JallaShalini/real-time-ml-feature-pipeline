from kafka import KafkaConsumer
import os
import json

BOOTSTRAP = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
TOPIC = os.getenv('FEATURE_STORE_TOPIC', 'feature-store')

def create_consumer():
    consumer = KafkaConsumer(TOPIC, bootstrap_servers=BOOTSTRAP, auto_offset_reset='latest')
    return consumer
