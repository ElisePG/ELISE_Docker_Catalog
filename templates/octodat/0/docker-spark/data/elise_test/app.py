from flask import Flask, render_template

from kafka import KafkaConsumer
import json

app = Flask(__name__)
control_consumer = KafkaConsumer("new_pairing", bootstrap_servers=['kafka:9092'], value_deserializer=lambda m: json.loads(m.decode('utf-8')), api_version=(0,10))
data_consumer = KafkaConsumer(bootstrap_servers=['kafka:9092'], value_deserializer=lambda m: json.loads(m.decode('utf-8')), api_version=(0,10))

sensor_mapping = {"Raum1": "", "Raum2": ""}

@app.route('/')
def hello_world():
    statuslist = []
    msg = next(control_consumer)
    if msg != {}:
        data_consumer.subscribe("{}_control".format(msg.value["guid"]))
        if msg.value["sensorid"] == "":
            sensor_mapping["Raum1"] = msg.value["guid"]
        elif msg.value["sensorid"] == "":
            sensor_mapping["Raum2"] = msg.value["guid"]
    msg = next(data_consumer)
    if msg != {}:
        if msg.value["GUID"] == sensor_mapping["Raum1"]:
            statuslist.append(("Raum 1", "Hilfe benötigt"),)
        else:
            statuslist.append(("Raum 1", "Alles ok"), )
        if msg.value["GUID"] == sensor_mapping["Raum2"]:
            statuslist.append(("Raum 2", "Hilfe benötigt"), )
        else:
            statuslist.append(("Raum 2", "Alles ok"), )
    return render_template("index.html", status=statuslist)

if __name__ == '__main__':
    app.run()
