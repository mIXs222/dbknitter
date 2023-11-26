#!/bin/bash

# Update package list and install system dependencies
sudo apt-get update
sudo apt-get install -y python3-pip libmysqlclient-dev

# Install Python dependencies
pip3 install pymysql pymongo pandas redis direct-redis msgpack-python
