#!/bin/bash

# Ensure the availability of Python and pip
sudo apt-get update
sudo apt-get install -y python3 python3-pip

# Install required Python packages
pip3 install pandas pymysql pymongo direct_redis
