#!/bin/bash
apt-get update
apt-get install -y python3-pip
pip3 install mysql-connector-python 
pip3 install pymongo
python3 main.py
