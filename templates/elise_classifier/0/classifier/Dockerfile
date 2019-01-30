FROM smizy/keras:2.0-cpu-alpine
MAINTAINER Jonas Pöhler <jonas.poehler@student.uni-siegen.de>
RUN apk add --update alpine-sdk python3-dev 
RUN pip3 install twisted numpy 
RUN mkdir /classification
WORKDIR /classification
ADD ./classification_api.py /classification
ADD ./model.h5 /classification
CMD ["python3", "classification_api.py"]
