#!/bin/bash

# Basic update and pip install
sudo apt-get update
sudo apt-get install -y python3-pip

# Install required Python libraries for the script
pip3 install pandas pymysql pymongo direct-redis
