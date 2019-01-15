from kafka import KafkaConsumer
import csv
import json
import time
import argparse
parser = argparse.ArgumentParser()
parser.add_argument("--guid")
parser.add_argument("--sensorid")
parser.add_argument("--framelength")
args = parser.parse_args()

data_topic = "{}_restructured".format(args.sensorid)
form_topic = "{}_form_data".format(args.guid)

consumer = KafkaConsumer([data_topic, form_topic], bootstrap_servers=['kafka:9092'], value_deserializer=lambda m: json.loads(m.decode('utf-8')), api_version=(0,10))
session_log_file = open("{}_{}_{}_elise_data_log.csv".format(time.time(), args.guid, args.sensorid), "w")
form_log_file = open("{}_{}_{}_elise_form_log.csv".format(time.time(), args.guid, args.sensorid), "w")
session_log_writer = csv.DictWriter(session_log_file, fieldnames=["Timestamp", "Temperatur", "GSR", "EOG1", "EOG2", "EEG1", "EEG2", "RED_raw", "IR_raw", "HeartRate", "SpO2"])
session_log_writer.writeheader()
form_log_writer = csv.DictWriter(form_log_file, fieldnames=["Timestamp", "State", "Type", "Data"])
form_log_writer.writeheader()

for msg in consumer:
    print(msg)
    data = msg.value
    if msg.topic == data_topic:
        session_log_writer.writerows([{"Timestamp": msg.timestamp,
                                       "Temperatur": data["Temperature"],
                                       "GSR": data["GSR"][i],
                                       "EOG1": data["EOG1"][i],
                                       "EOG2": data["EOG2"][i],
                                       "EEG1": data["EEG1"][i],
                                       "EEG2": data["EEG2"][i],
                                       "RED_raw": data["RED_RAW"][i],
                                       "IR_raw": data["IR_RAW"][i],
                                       "HeartRate": 0,
                                       "SpO2": 0
                                       } for i in range(args.framelength)])
    elif msg.topic == form_topic:
        form_log_writer.writerow({"Timestamp": msg.timestamp,
                                 "State": data["State"],
                                 "Type": data["Type"],
                                 "Data": data["Data"],
                                })


