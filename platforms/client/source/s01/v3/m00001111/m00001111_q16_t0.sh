#!/bin/bash
apt-get update && \
apt-get install -y python3-pip && \
pip3 install --upgrade pip setuptools && \
pip3 install mysql-connector-python pymongo pandas
