from kafka import KafkaConsumer, KafkaProducer
import csv
import json
import re
import threading
import time
FRAME_LENGTH = 75
consumer = KafkaConsumer(bootstrap_servers=['kafka:9092'], value_deserializer=lambda m: json.loads(m.decode('utf-8')), api_version=(0,10))
producer = KafkaProducer(bootstrap_servers=["kafka:9092"], value_serializer=lambda m: json.dumps(m).encode('utf-8'), api_version=(0,10))
    
def register_kafka_listener(topic):
    def poll():
        consumer = KafkaConsumer(topic, bootstrap_servers=['kafka:9092'], value_deserializer=lambda m: json.loads(m.decode('utf-8')), api_version=(0,10))
        producer = KafkaProducer(bootstrap_servers=["kafka:9092"], value_serializer=lambda m: json.dumps(m).encode('utf-8'), api_version=(0,10))
        consumer.poll(timeout=6000)
        for msg in consumer:
            for i in range(0, FRAME_LENGTH, 15):
                producer.send("{}_atomic".format(topic.split("_")[0]), {"Temperatur": msg.value["Temperatur"], "GSR": msg.value["GSR"][i:i+15-1],
                time.sleep(0.0625)
            
    t1 = threading.Thread(target=poll)
    t1.start()
    

known_channels = list()

while True:
    for topic in (list(set(consumer.topics()) - set(known_channels))):
        if re.match(r'.{12,15}_restructured'):
            register_kafka_listener(topic)
            known_channels.append(topic)


