#!/bin/bash 

spark-submit /usr/spark-2.4.0/pydata/twisted_udp_api.py &
spark-submit /usr/spark-2.4.0/pydata/kafka_temperature_csv_consumer.py &
spark-submit /usr/spark-2.4.0/pydata/kafka_gsr_csv_consumer.py &
spark-submit /usr/spark-2.4.0/pydata/kafka_eog1_csv_consumer.py &
spark-submit /usr/spark-2.4.0/pydata/kafka_eog2_csv_consumer.py &
spark-submit /usr/spark-2.4.0/pydata/kafka_eeg1_csv_consumer.py &
spark-submit /usr/spark-2.4.0/pydata/kafka_eeg2_csv_consumer.py &
spark-submit /usr/spark-2.4.0/pydata/kafka_redraw_csv_consumer.py &
spark-submit /usr/spark-2.4.0/pydata/kafka_irraw_csv_consumer.py &
spark-submit /usr/spark-2.4.0/pydata/kafka_formdata_csv_consumer.py &
