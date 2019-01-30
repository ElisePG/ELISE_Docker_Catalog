#!/usr/bin/env python3

from twisted.internet.protocol import DatagramProtocol
from twisted.internet import reactor
from twisted.internet import defer
from twisted.application import internet, service
import json
from socket import SOL_SOCKET, SO_BROADCAST
from keras.layers import Dense, BatchNormalization, Flatten, Input
from keras.models import Model, Sequential, load_model
from keras.optimizers import Adadelta
from keras import backend as K
import tensorflow as tf
import numpy as np

def precision(y_true, y_pred):
   """Precision metric.    Only computes a batch-wise average of precision.    Computes the precision, a metric for multi-label classification of
   how many selected items are relevant.
   """
   true_positives = K.sum(K.round(K.clip(y_true * y_pred, 0, 1)))
   predicted_positives = K.sum(K.round(K.clip(y_pred, 0, 1)))
   precision = true_positives / (predicted_positives + K.epsilon())
   return precision
def recall(y_true, y_pred):
   """Recall metric.    Only computes a batch-wise average of recall.    Computes the recall, a metric for multi-label classification of
   how many relevant items are selected.
   """
   true_positives = K.sum(K.round(K.clip(y_true * y_pred, 0, 1)))
   possible_positives = K.sum(K.round(K.clip(y_true, 0, 1)))
   recall = true_positives / (possible_positives + K.epsilon())
   return recall
def fbeta_score(y_true, y_pred, beta=1):
   """Computes the F score.    The F score is the weighted harmonic mean of precision and recall.
   Here it is only computed as a batch-wise average, not globally.    This is useful for multi-label classification, where input samples can be
   classified as sets of labels. By only using accuracy (precision) a model
   would achieve a perfect score by simply assigning every class to every
   input. In order to avoid this, a metric should penalize incorrect class
   assignments as well (recall). The F-beta score (ranged from 0.0 to 1.0)
   computes this, as a weighted mean of the proportion of correct class
   assignments vs. the proportion of incorrect class assignments.    With beta = 1, this is equivalent to a F-measure. With beta < 1, assigning
   correct classes becomes more important, and with beta > 1 the metric is
   instead weighted towards penalizing incorrect class assignments.
   """
   if beta < 0:
       raise ValueError('The lowest choosable beta is zero (only precision).')    # If there are no true positives, fix the F score at 0 like sklearn.
   if K.sum(K.round(K.clip(y_true, 0, 1))) == 0:
       return 0
   p = precision(y_true, y_pred)
   r = recall(y_true, y_pred)
   bb = beta ** 2
   fbeta_score = (1 + bb) * (p * r) / (bb * p + r + K.epsilon())
   return fbeta_score

def fmeasure(y_true, y_pred):
   """Computes the f-measure, the harmonic mean of precision and recall.    Here it is only computed as a batch-wise average, not globally.
   """
   return fbeta_score(y_true, y_pred, beta=1)


class EliseDataUDP(DatagramProtocol):
    noisy = False
    message_buffer = {
    "Temperature": list(),
    "GSR": list(),
    "EOG1": list(),
    "EOG2": list(),
    "EEG1": list(),
    "EEG2": list(),
    "RED_raw": list(),
    "IR_raw": list(),
    }

    def startProtocol(self):
        self.transport.socket.setsockopt(SOL_SOCKET, SO_BROADCAST, True)
        self.model = load_model('model.h5', custom_objects={'fmeasure': fmeasure})
        self.graph = tf.get_default_graph()
        print("---API online---")

    def datagramReceived(self, datagram, address):
        datalist = str(datagram, encoding="utf-8").split(";")
        if datalist[0] == "ELISE_DATA":
            if "Temperature" in datalist:
                self.message_buffer["Temperature"].extend([datalist[6] for i in range(75)])
            if "GSR" in datalist:
                self.message_buffer["GSR"].extend(datalist[8].split(",")[:-1])
            if "EOG1" in datalist:
                self.message_buffer["EOG1"].extend(datalist[10].split(",")[:-1])
            if "EOG2" in datalist:
                self.message_buffer["EOG2"].extend(datalist[7].split(",")[:-1])
            if "EEG1" in datalist:
                self.message_buffer["EEG1"].extend(datalist[9].split(",")[:-1])
            if "EEG2" in datalist:
                self.message_buffer["EEG2"].extend(datalist[6].split(",")[:-1])
            if "RED_RAW" in datalist:
                self.message_buffer["RED_raw"].extend(datalist[8].split(",")[:-1])
            if "IR_RAW" in datalist:
                self.message_buffer["IR_raw"].extend(datalist[7].split(",")[:-1])

        if all(len(self.message_buffer[key]) > 416 for key in self.message_buffer.keys()):
            with self.graph.as_default():
                self.model._make_predict_function()
                prediction = self.model.predict(np.reshape(np.array([message_buffer["Temperature"], message_buffer["GSR"], message_buffer["EOG1"], message_buffer["EOG2"], message_buffer["EEG1"], message_buffer["EEG2"], message_buffer["RED_raw"], message_buffer["IR_raw"], message_buffer["EEG2"],]), (1, 416, 9, 1,)))
                for entry in self.message_buffer.keys():
                    self.message_buffer[entry] = self.message_buffer[entry][416:]
                self.transport.write("ELISE_PREDICTION;{}".format(prediction), ('255.255.255.255', 5002))
              

def main():
    reactor.listenUDP(5001, EliseDataUDP())
    reactor.run()

if __name__ == '__main__':
    main()
