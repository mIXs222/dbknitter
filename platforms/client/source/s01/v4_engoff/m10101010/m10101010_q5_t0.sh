#!/bin/bash

# Update package lists
apt-get update

# Install Python 3 and PIP
apt-get install -y python3 python3-pip

# Install pymysql and pymongo with PIP
pip3 install pymysql pymongo
