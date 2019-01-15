from kafka import KafkaConsumer, KafkaProducer
import json
import pandas as pd
import signal
import sys
import time
import numpy as np




def signal_handler(sig, frame):
        print('You pressed Ctrl+C!')
        consumer.unsubscribe()
        control_consumer.unsubscribe()
        producer.send("control", {"message": "Goodbye"})
        sys.exit(0)


signal.signal(signal.SIGINT, signal_handler)
FRAME_LENGTH = 75
consumer = KafkaConsumer(bootstrap_servers=['kafka:9092'], value_deserializer=lambda m: json.loads(m.decode('utf-8')))
#consumer.subscribe(pattern='.{12,15}_raw_data')

class PackageOrderDisturbed(Exception):

    def __init__(self, package_id, exp_packet, data):
        super().__init__("Expected Packet {}/{} got: {}".format(package_id, exp_packet, data))


class data_package:

    temperature = None
    guid = None
    gsr = None
    eog1 = None
    eog2 = None
    eeg1 = None
    eeg2 = None
    red_raw = None
    ir_raw = None
    heart_rate = None
    spo2 = None
    packet1_flag = False
    packet2_flag = False
    packet3_flag = False
    packet4_flag = False
    packet_id = None
    expected_packet = 1

    def __init__(self, frame_length: int):
        self.temperature, self.gsr, self.eog1, self.eog2, self.eeg1, self.eeg2, \
        self.red_raw, self.ir_raw, self.heart_rate, self.spo2 = np.zeros(frame_length)

    def consume_data(self, data: dict):
        if not self.packet1_flag:
            self.guid = data["SensorGUID"]
            if "Temperature" in data:
                self.packet_id = data["PacketNr"]
                self.packet1_flag = True
                self.expected_packet = 2
                self.temperature = [data["Temperature"] for i in range(len(self.temperature))]
                self.gsr = data["GSR"]
                self.eog1 = data["EOG1"]
            else:
                raise PackageOrderDisturbed(package_id=self.packet_id, exp_packet=self.expected_packet, data=data)
        elif self.packet1_flag and not self.packet2_flag:
            if "EOG2" in data:
                self.packet2_flag = True
                self.expected_packet = 3
                self.eog2 = data["EOG2"]
                self.eeg1 = data["EEG1"]
            else:
                raise PackageOrderDisturbed(package_id=self.packet_id, exp_packet=self.expected_packet, data=data)
        elif self.packet2_flag and not self.packet3_flag:
            if "EEG2" in data:
                self.packet3_flag = True
                self.expected_packet = 4
                self.eeg2 = data["EEG2"]
                self.red_raw = data["RED_RAW"]
            else:
                raise PackageOrderDisturbed(package_id=self.packet_id, exp_packet=self.expected_packet, data=data)
        elif self.packet3_flag and not self.packet4_flag:
            if "IR_RAW" in data:
                self.packet4_flag = True
                self.ir_raw = data["IR_RAW"]
            else:
                raise PackageOrderDisturbed(package_id=self.packet_id, exp_packet=self.expected_packet, data=data)

    def check_complete(self):
        if self.packet1_flag and self.packet2_flag and self.packet3_flag and self.packet4_flag:
            return True
        return False

    def to_json(self):
        return {"Temperatur": self.temperature, "GSR": self.gsr, "EOG1": self.eog1, "EOG2": self.eog2, "EEG1": self.eeg1, "EEG2": self.eeg2, "RED_raw": self.red_raw, "IR_raw": self.ir_raw}




def register_kafka_listener(topic):
    def poll():
        consumer = KafkaConsumer(topic, bootstrap_servers=['kafka:9092'], value_deserializer=lambda m: json.loads(m.decode('utf-8')), api_version=(0,10))
        producer = KafkaProducer(bootstrap_servers=["kafka:9092"], value_serializer=lambda m: json.dumps(m).encode('utf-8'), api_version=(0,10))
        consumer.poll(timeout=6000)
        while True:
            try:
                pack = data_package(FRAME_LENGTH)
                for msg in consumer:
                    pack.consume(msg.value)
                    if pack.check_complete():
                        break
                producer.send("{}_restructured".format(pack.guid), pack.to_json())
            except PackageOrderDisturbed as pdis:
                print(pdis)
                continue
    t1 = threading.Thread(target=poll)
    t1.start()


known_channels = list()

while True:
    for topic in (list(set(consumer.topics()) - set(known_channels))):
        if re.match(r'.{12,15}_raw_data'):
            register_kafka_listener(topic)
            known_channels.append(topic)
    
