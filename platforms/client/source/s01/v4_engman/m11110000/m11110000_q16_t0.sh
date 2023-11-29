#!/bin/bash

# Update system
apt-get update -y

# Install Python 3 and pip
apt-get install python3 python3-pip -y

# Install pymysql and pymongo
pip3 install pymysql pymongo
