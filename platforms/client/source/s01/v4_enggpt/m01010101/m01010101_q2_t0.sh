#!/bin/bash

# Update package list and install python3 and pip
apt-get update
apt-get install -y python3 python3-pip

# Install pymysql and pymongo using pip
pip3 install pymysql pymongo
