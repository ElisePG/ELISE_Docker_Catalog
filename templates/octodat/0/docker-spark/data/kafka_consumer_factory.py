from kafka import KafkaConsumer
import csv
import json
import subprocess

FRAME_LENGTH = 50
consumer = KafkaConsumer(['new_sensor','new_vr_induction'], bootstrap_servers=['kafka:9092'], api_version=(0,10))

new_sensor = None
new_vr = None

for msg in consumer:
    json_data = json.loads(msg.value)
    if msg.topic == 'new_sensor':
        new_sensor = json_data["data"]
    if msg.topic == 'new_vr_induction':
        new_vr = json_data["data"]
    if new_sensor != None and new_vr != None:
        print("Calling new consumer")
        subprocess.call(["spark-submit", "kafka_csv_consumer.py --guid {} --sensorid {} --framelength {}".format(new_vr, new_sensor, FRAME_LENGTH)])
        new_senor = None
        new_vr = None

        
