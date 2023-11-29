#!/bin/bash

# Update repositories and install Python 3 and pip
sudo apt-get update
sudo apt-get install -y python3 python3-pip

# Install pymysql and pymongo using pip
pip3 install pymysql pymongo
