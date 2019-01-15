#!/usr/bin/env python3

from twisted.internet.protocol import DatagramProtocol
from twisted.internet import reactor
from twisted.internet import defer
from twisted.application import internet, service
import json
from kafka import KafkaProducer
import time


class Logger:
    
    def __init__(self):
        self.templogdict = dict()
        self.gsrlogdict = dict()
        self.eog1logdict = dict()
        self.eog2logdict = dict()
        self.eeg1logdict = dict()
        self.eeg2logdict = dict()
        self.redrawlogdict = dict()
        self.irrawlogdict = dict()

    def init_new_logger(self, guid, typ):
        if typ == "temp":
            datafile = open("/var/data/{}_{}_temperatur.csv".format(time.time(), guid), "w")
            self.logdict[guid] = (datafile, csv.DictWriter(datafile, fieldnames=["Timestamp", "PacketNr", "Temperatur"]))
            self.logdict[guid][1].writeheader()
        elif typ == "gsr":
            datafile = open("/var/data/{}_{}_gsr.csv".format(time.time(), guid), "w")
            self.gsrlogdict[guid] = (datafile, csv.DictWriter(datafile, fieldnames=["Timestamp", "PacketNr", "GSR"]))
            self.gsrlogdict[guid][1].writeheader()
        elif typ == "eog1":
            datafile = open("/var/data/{}_{}_eog1.csv".format(time.time(), guid), "w")
            self.eog1logdict[guid] = (datafile, csv.DictWriter(datafile, fieldnames=["Timestamp", "PacketNr", "EOG1"]))
            self.eog1logdict[guid][1].writeheader()
        elif typ == "eog2":
            datafile = open("/var/data/{}_{}_eog2.csv".format(time.time(), guid), "w")
            self.eog2logdict[guid] = (datafile, csv.DictWriter(datafile, fieldnames=["Timestamp", "PacketNr", "EOG2"]))
            self.eog2logdict[guid][1].writeheader()
        elif typ == "eeg1":
            datafile = open("/var/data/{}_{}_eeg1.csv".format(time.time(), guid), "w")
            self.eeg1logdict[guid] = (datafile, csv.DictWriter(datafile, fieldnames=["Timestamp", "PacketNr", "EEG1"]))
            self.eeg1logdict[guid][1].writeheader()
        elif typ == "eeg2":
            datafile = open("/var/data/{}_{}_eeg2.csv".format(time.time(), guid), "w")
            self.eeg2logdict[guid] = (datafile, csv.DictWriter(datafile, fieldnames=["Timestamp", "PacketNr", "EEG2"]))
            self.eeg2logdict[guid][1].writeheader()
        elif typ == "redraw":
            datafile = open("/var/data/{}_{}_red_raw.csv".format(time.time(), guid), "w")
            self.redrawlogdict[guid] = (datafile, csv.DictWriter(datafile, fieldnames=["Timestamp", "PacketNr", "RED_RAW"]))
            self.redrawlogdict[guid][1].writeheader()
        elif typ == "irraw":
            datafile = open("/var/data/{}_{}_ir_raw.csv".format(time.time(), guid), "w")
            self.irrawlogdict[guid] = (datafile, csv.DictWriter(datafile, fieldnames=["Timestamp", "PacketNr", "IR_RAW"]))
            self.irrawlogdict[guid][1].writeheader()




class EliseDataUDP(DatagramProtocol):
    noisy = False

    def __init__(self):
        #self.kafka_producer = KafkaProducer(bootstrap_servers=["kafka:9092"], value_serializer=lambda m: json.dumps(m).encode('utf-8'), api_version=(0,10))
        #self.setBroadcastAllowed = True
        self.system_mapping = dict()
        self.new_vr = None
        self.new_sensor = None
        self.CsvLog = Logger()

    def datagramReceived(self, datagram, address):
        datalist = str(datagram, encoding="utf-8").split(";")
        if datalist[0] == "ELISE_OFFER" and datalist[1] == "SENSOR":
            #self.kafka_producer.send("new_sensor", {"guid": datalist[2]})
            self.new_sensor = datalist[2]
            if self.new_vr is not None and self.new_sensor is not None:
                self.system_mapping[self.new_vr] = self.new_sensor
                #self.kafka_producer.send("new_pairing", {"guid": self.new_vr, "sensorid": self.new_sensor})
                self.new_sensor = None
                self.new_vr = None
            #self.write(bytes("ELISE_ACK;{};{}".format("127.0.0.1", "0"), encoding="utf-8"), address)
        elif datalist[0] == "ELISE_DATA":
            data = {
                "SensorGUID": datalist[2],
                "PacketNr": datalist[4].replace(",", ""),
            }
            if "Temperature" in datalist:
                data.update({"Temperature": datalist[6]})
                #self.kafka_producer.send("temperature", data)
                if data["SensorGUID"] in self.CsvLog.templogdict:
                    self.CsvLog.templogdict[data["SensorGUID"]][1].writerows([{"Timestamp": time.time(),
                                           "PacketNr": data["PacketNr"],
                                           "Temperatur": data["Temperature"]
                                           }])
                    self.CsvLog.templogdict[data["SensorGUID"]][0].flush()
                else:
                    self.CsvLog.init_new_logger(data["SensorGUID"], "temp")
                    self.CsvLog.templogdict[data["SensorGUID"]][1].writerows([{"Timestamp": time.time(),
                                           "PacketNr": data["PacketNr"],
                                           "Temperatur": data["Temperature"]
                                           }])
                    self.CsvLog.templogdict[data["SensorGUID"]][0].flush()
            if "GSR" in datalist:
                data.update({"GSR": datalist[8].split(",")[:-1]})
                #self.kafka_producer.send("gsr", data)
                if data["SensorGUID"] in self.CsvLog.gsrlogdict:
                    self.CsvLog.gsrlogdict[data["SensorGUID"]][1].writerows([{"Timestamp": time.time(),
                                           "PacketNr": data["PacketNr"],
                                           "Temperatur": data["Temperature"]
                                           }])
                    self.CsvLog.gsrlogdict[data["SensorGUID"]][0].flush()
                else:
                    self.CsvLog.init_new_logger(data["SensorGUID"], "gsr")
                    self.CsvLog.gsrlogdict[data["SensorGUID"]][1].writerows([{"Timestamp": time.time(),
                                           "PacketNr": data["PacketNr"],
                                           "Temperatur": data["Temperature"]
                                           }])
                    self.CsvLog.gsrlogdict[data["SensorGUID"]][0].flush()
            if "EOG1" in datalist:
                data.update({"EOG1": datalist[10].split(",")[:-1]})
                #self.kafka_producer.send("eog1", data)
                if data["SensorGUID"] in self.CsvLog.eog1logdict:
                    self.CsvLog.eog1logdict[data["SensorGUID"]][1].writerows([{"Timestamp": time.time(),
                                           "PacketNr": data["PacketNr"],
                                           "EOG1": data["EOG1"][i]
                                           } for i in range(len(data["EOG1"]))])
                    self.CsvLog.eog1logdict[data["SensorGUID"]][0].flush()
                else:
                    self.CsvLog.init_new_logger(data["SensorGUID"], "eog1")
                    self.CsvLog.eog1logdict[data["SensorGUID"]][1].writerows([{"Timestamp": time.time(),
                                           "PacketNr": data["PacketNr"],
                                           "EOG1": data["EOG1"][i]	
                                           } for i in range(len(data["EOG1"]))])
                    self.CsvLog.eog1logdict[data["SensorGUID"]][0].flush()
            if "EOG2" in datalist:
                data.update({"EOG2": datalist[7].split(",")[:-1]})
                #self.kafka_producer.send("eog2", data)
                if data["SensorGUID"] in self.CsvLog.eog2logdict:
                    self.CsvLog.eog2logdict[data["SensorGUID"]][1].writerows([{"Timestamp": time.time(),
                                           "PacketNr": data["PacketNr"],
                                           "EOG2": data["EOG2"][i]
                                           } for i in range(len(data["EOG2"]))])
                    self.CsvLog.eog2logdict[data["SensorGUID"]][0].flush()
                else:
                    self.CsvLog.init_new_logger(data["SensorGUID"], "eog2")
                    self.CsvLog.eog2logdict[data["SensorGUID"]][1].writerows([{"Timestamp": time.time(),
                                           "PacketNr": data["PacketNr"],
                                           "EOG2": data["EOG2"][i]
                                           } for i in range(len(data["EOG2"]))])
                    self.CsvLog.eog2logdict[data["SensorGUID"]][0].flush()
            if "EEG1" in datalist:
                data.update({"EEG1": datalist[9].split(",")[:-1]})
                #self.kafka_producer.send("eeg1", data)
                if data["SensorGUID"] in self.CsvLog.eeg1logdict:
                    self.CsvLog.eeg1logdict[data["SensorGUID"]][1].writerows([{"Timestamp": time.time(),
                                           "PacketNr": data["PacketNr"],
                                           "EEG1": data["EEG1"][i]
                                           } for i in range(len(data["EEG1"]))])
                    self.CsvLog.eeg1logdict[data["SensorGUID"]][0].flush()
                else:
                    self.CsvLog.init_new_logger(data["SensorGUID"], "eeg1")
                    self.CsvLog.eeg1logdict[data["SensorGUID"]][1].writerows([{"Timestamp": time.time(),
                                           "PacketNr": data["PacketNr"],
                                           "EEG1": data["EEG1"][i]
                                           } for i in range(len(data["EEG1"]))])
                    self.CsvLog.eeg1logdict[data["SensorGUID"]][0].flush()
            if "EEG2" in datalist:
                data.update({"EEG2": datalist[6].split(",")[:-1]})
                #self.kafka_producer.send("eeg2", data)
                if data["SensorGUID"] in self.CsvLog.eeg2logdict:
                    self.CsvLog.eeg2logdict[data["SensorGUID"]][1].writerows([{"Timestamp": time.time(),
                                           "PacketNr": data["PacketNr"],
                                           "EEG2": data["EEG2"][i]
                                           } for i in range(len(data["EEG2"]))])
                    self.CsvLog.eeg2logdict[data["SensorGUID"]][0].flush()
                else:
                    self.CsvLog.init_new_logger(data["SensorGUID"], "eeg2")
                    self.CsvLog.eeg2logdict[data["SensorGUID"]][1].writerows([{"Timestamp": time.time(),
                                           "PacketNr": data["PacketNr"],
                                           "EEG2": data["EEG2"][i]
                                           } for i in range(len(data["EEG2"]))])
                    self.CsvLog.eeg2logdict[data["SensorGUID"]][0].flush()
            if "RED_RAW" in datalist:
                data.update({"RED_RAW": datalist[8].split(",")[:-1]})
                #self.kafka_producer.send("red_raw", data)
                if data["SensorGUID"] in self.CsvLog.redrawlogdict:
                    self.CsvLog.redrawlogdict[data["SensorGUID"]][1].writerows([{"Timestamp": time.time(),
                                           "PacketNr": data["PacketNr"],
                                           "RED_RAW": data["RED_RAW"][i]
                                           } for i in range(len(data["RED_RAW"]))])
                    self.CsvLog.redrawlogdict[data["SensorGUID"]][0].flush()
                else:
                    self.CsvLog.init_new_logger(data["SensorGUID"], "redraw")
                    self.CsvLog.redrawlogdict[data["SensorGUID"]][1].writerows([{"Timestamp": time.time(),
                                           "PacketNr": data["PacketNr"],
                                           "RED_RAW": data["RED_RAW"][i]
                                           } for i in range(len(data["RED_RAW"]))])
                    self.CsvLog.redrawlogdict[data["SensorGUID"]][0].flush()
            if "IR_RAW" in datalist:
                data.update({"IR_RAW": datalist[7].split(",")[:-1]})
                #self.kafka_producer.send("ir_raw", data)
                if data["SensorGUID"] in self.CsvLog.irrawlogdict:
                    self.CsvLog.irrawlogdict[data["SensorGUID"]][1].writerows([{"Timestamp": time.time(),
                                           "PacketNr": data["PacketNr"],
                                           "IR_RAW": data["IR_RAW"][i]
                                           } for i in range(len(data["IR_RAW"]))])
                    self.CsvLog.irrawlogdict[data["SensorGUID"]][0].flush()
                else:
                    self.CsvLog.init_new_logger(data["SensorGUID"], "irraw")
                    self.CsvLog.irrawlogdict[data["SensorGUID"]][1].writerows([{"Timestamp": time.time(),
                                           "PacketNr": data["PacketNr"],
                                           "IR_RAW": data["IR_RAW"][i]
                                           } for i in range(len(data["IR_RAW"]))])
                    self.CsvLog.irrawlogdict[data["SensorGUID"]][0].flush()
            #self.kafka_producer.send("{}_raw_data".format(datalist[2]), data)
            #self.kafka_producer.flush()

class EliseFormUDP(DatagramProtocol):
    noisy = False

    def __init__(self):
        pass
        #self.kafka_producer = KafkaProducer(bootstrap_servers=["kafka:9092"], value_serializer=lambda m: json.dumps(m).encode('utf-8'), api_version=(0,10))
        #self.setBroadcastAllowed = True

    def datagramReceived(self, datagram, address):
        datalist = str(datagram, encoding="utf-8").split(";")
        if datalist[0] == "ELISE_DISCOVER" and datalist[1] == "VR":
            self.kafka_producer.send("new_vr_induction", {"guid": datalist[2]})
            self.new_vr = datalist[2]
            if self.new_vr is not None and self.new_sensor is not None:
                self.system_mapping[self.new_vr] = self.new_sensor
                self.kafka_producer.send("new_pairing", {"guid": self.new_vr, "sensorid": self.new_sensor})
                self.new_sensor = None
                self.new_vr = None
            #self.write(bytes("ELISE_OFFER;{};{};{}".format(datalist[2], str.concat(".", address[0].split(".")[0:2] + ["255"]), "0"), encoding="utf-8"), address)
        elif datalist[0] == "ELISE_FORM" and datalist[2] == "Emotion":
            #self.kafka_producer.send("{}_form_data".format(datalist[-1]), {
            #    "State": datalist[1],
            #    "Type": datalist[2],
            #    "Data": [datalist[3], datalist[4], datalist[5], datalist[6]]
            #})
            self.kafka_producer.send("form_data", {
                "GUID": datalist[-1],
                "State": datalist[1],
                "Type": datalist[2],
                "Data": [datalist[3], datalist[4], datalist[5], datalist[6]]
            })
        elif datalist[0] == "ELISE_FORM" and datalist[2] == "Circumplex":
            #self.kafka_producer.send("{}_form_data".format(datalist[-1]), {
            #    "State": datalist[1],
            #    "Type": datalist[2],
            #    "Data": [datalist[3], datalist[4], datalist[5]]
            #})
            self.kafka_producer.send("form_data", {
                "GUID": datalist[-1],
                "State": datalist[1],
                "Type": datalist[2],
                "Data": [datalist[3], datalist[4], datalist[5]]
            })
        elif datalist[0] == "ELISE_HELP":
            self.kafka_producer.send("{}_control".format(datalist[-1]), {
                "GUID": datalist[-1],
                "Assistance": True
            })

def main():
    reactor.listenUDP(5001, EliseDataUDP())
    #reactor.listenUDP(5002, EliseFormUDP())
    reactor.run()

if __name__ == '__main__':
    main()
