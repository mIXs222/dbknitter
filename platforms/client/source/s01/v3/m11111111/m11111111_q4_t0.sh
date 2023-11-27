#!/bin/bash

# Update the packages list
apt-get update -y

# Install Python3 and PIP
apt-get install -y python3-pip

# Install the necessary python packages
pip3 install mysql-connector-python
pip3 install pymongo
