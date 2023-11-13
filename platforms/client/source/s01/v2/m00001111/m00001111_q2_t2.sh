#!/bin/bash

# update package lists
apt-get update -y

# Install python and pip
apt-get install python3.8 -y

curl https://bootstrap.pypa.io/get-pip.py -o get-pip.py
python3 get-pip.py

# Install python mysql connector
pip install mysql-connector-python

# Install pymongo
pip install pymongo

# Install pandas
pip install pandas
