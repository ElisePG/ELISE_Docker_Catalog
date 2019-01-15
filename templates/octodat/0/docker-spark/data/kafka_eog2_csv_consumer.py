from kafka import KafkaConsumer
import csv
import json
import time



consumer = KafkaConsumer("eog2", bootstrap_servers=['kafka:9092'], value_deserializer=lambda m: json.loads(m.decode('utf-8')), api_version=(0,10))
#session_log_file = open("{}_{}_{}_elise_data_log.csv".format(time.time(), args.guid, args.sensorid), "w")
#form_log_file = open("{}_{}_{}_elise_form_log.csv".format(time.time(), args.guid, args.sensorid), "w")
#session_log_writer = csv.DictWriter(session_log_file, fieldnames=["Timestamp", "PacketNr", "Temperatur"])
#session_log_writer.writeheader()
#form_log_writer = csv.DictWriter(form_log_file, fieldnames=["Timestamp", "State", "Type", "Data"])
#form_log_writer.writeheader()

class Logger:
    
    def __init__(self):
        self.logdict = dict()

    def init_new_logger(self, guid):
        datafile = open("/var/data/{}_{}_eog2.csv".format(time.time(), guid), "w")
        self.logdict[guid] = (datafile, csv.DictWriter(datafile, fieldnames=["Timestamp", "PacketNr", "EOG2"]))
        self.logdict[guid][1].writeheader()


CsvLog = Logger()

for msg in consumer:
    try:
        data = msg.value
        if data["SensorGUID"] in CsvLog.logdict:
            CsvLog.logdict[data["SensorGUID"]][1].writerows([{"Timestamp": msg.timestamp,
                                           "PacketNr": data["PacketNr"],
                                           "EOG2": data["EOG2"][i]
                                           } for i in range(len(data["EOG2"]))])
            CsvLog.logdict[data["SensorGUID"]][0].flush()
        else:
            CsvLog.init_new_logger(data["SensorGUID"])
            CsvLog.logdict[data["SensorGUID"]][1].writerows([{"Timestamp": msg.timestamp,
                                           "PacketNr": data["PacketNr"],
                                           "EOG2": data["EOG2"][i]
                                           } for i in range(len(data["EOG2"]))])
            CsvLog.logdict[data["SensorGUID"]][0].flush()
    except Exception as e:
        print(e)

