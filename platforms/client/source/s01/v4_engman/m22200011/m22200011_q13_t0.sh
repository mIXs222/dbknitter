#!/bin/bash

# Update package list
apt-get update

# Install Python 3 and pip
apt-get install -y python3 python3-pip

# Install required Python libraries
pip3 install pymysql pymongo
