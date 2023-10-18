FROM ubuntu:20.04

RUN apt-get update
RUN apt-get upgrade --yes

# Install Python
RUN apt-get install python3.8 python-is-python3 python3-pip --yes
RUN python -m pip install pandas pymongo

# Install platform clients
RUN apt-get install mysql-client --yes

# Install DBKnitter
COPY ./requirements.txt /dbknitter/requirements.txt
RUN pip install -r /dbknitter/requirements.txt
COPY . /dbknitter
WORKDIR /dbknitter
RUN pip install .

WORKDIR /
ENTRYPOINT /bin/bash
