from kafka import KafkaProducer
import json
import random
from time import sleep
from datetime import datetime

producer = KafkaProducer(bootstrap_servers='127.0.0.1:9092',
                            value_serializer=lambda v: str(v).encode('utf-8'))

print("Ctrl+c to Stop")
while True:
    producer.send('kafka-python-topic', random.randint(1,999))