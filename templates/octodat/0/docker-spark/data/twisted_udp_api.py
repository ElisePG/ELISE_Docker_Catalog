#!/usr/bin/env python3

from twisted.internet.protocol import DatagramProtocol
from twisted.internet import reactor
from twisted.internet import defer
from twisted.application import internet, service
import json
from kafka import KafkaProducer

class EliseDataUDP(DatagramProtocol):
    noisy = False

    def __init__(self):
        self.kafka_producer = KafkaProducer(bootstrap_servers=["kafka:9092"], value_serializer=lambda m: json.dumps(m).encode('utf-8'), api_version=(0,10))
        #self.setBroadcastAllowed = True
        self.system_mapping = dict()
        self.new_vr = None
        self.new_sensor = None

    def datagramReceived(self, datagram, address):
        datalist = str(datagram, encoding="utf-8").split(";")
        if datalist[0] == "ELISE_OFFER" and datalist[1] == "SENSOR":
            self.kafka_producer.send("new_sensor", {"guid": datalist[2]})
            self.new_sensor = datalist[2]
            if self.new_vr is not None and self.new_sensor is not None:
                self.system_mapping[self.new_vr] = self.new_sensor
                self.kafka_producer.send("new_pairing", {"guid": self.new_vr, "sensorid": self.new_sensor})
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
                self.kafka_producer.send("temperature", data)
            if "GSR" in datalist:
                data.update({"GSR": datalist[8].split(",")[:-1]})
                self.kafka_producer.send("gsr", data)
            if "EOG1" in datalist:
                data.update({"EOG1": datalist[10].split(",")[:-1]})
                self.kafka_producer.send("eog1", data)
            if "EOG2" in datalist:
                data.update({"EOG2": datalist[7].split(",")[:-1]})
                self.kafka_producer.send("eog2", data)
            if "EEG1" in datalist:
                data.update({"EEG1": datalist[9].split(",")[:-1]})
                self.kafka_producer.send("eeg1", data)
            if "EEG2" in datalist:
                data.update({"EEG2": datalist[6].split(",")[:-1]})
                self.kafka_producer.send("eeg2", data)
            if "RED_RAW" in datalist:
                data.update({"RED_RAW": datalist[8].split(",")[:-1]})
                self.kafka_producer.send("red_raw", data)
            if "IR_RAW" in datalist:
                data.update({"IR_RAW": datalist[7].split(",")[:-1]})
                self.kafka_producer.send("ir_raw", data)
            #self.kafka_producer.send("{}_raw_data".format(datalist[2]), data)
            self.kafka_producer.flush()

class EliseFormUDP(DatagramProtocol):
    noisy = False

    def __init__(self):
        self.kafka_producer = KafkaProducer(bootstrap_servers=["kafka:9092"], value_serializer=lambda m: json.dumps(m).encode('utf-8'), api_version=(0,10))
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
    reactor.listenUDP(5002, EliseFormUDP())
    reactor.run()

if __name__ == '__main__':
    main()
