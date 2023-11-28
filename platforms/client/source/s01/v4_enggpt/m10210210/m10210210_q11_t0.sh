#!/bin/bash

# Update package list and install pip for Python 3
apt-get update
apt-get install -y python3-pip

# Install the required Python libraries
pip3 install pymysql pymongo
