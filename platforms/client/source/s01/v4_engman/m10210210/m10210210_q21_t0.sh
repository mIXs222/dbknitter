#!/bin/bash

# Update and install Python and pip
apt-get update
apt-get install -y python3 python3-pip

# Install pymysql and pymongo Python libraries
pip3 install pymysql pymongo
