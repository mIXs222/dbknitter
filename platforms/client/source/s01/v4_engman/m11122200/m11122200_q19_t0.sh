#!/bin/bash

# Update repositories and prepare the system
apt-get update -y

# Install pip for Python3 and MongoDB server
apt-get install -y python3-pip mongodb-server

# Install pymysql and pymongo using pip
pip3 install pymysql pymongo
