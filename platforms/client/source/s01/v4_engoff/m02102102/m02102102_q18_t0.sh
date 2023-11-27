#!/bin/bash

# Update the package list and install Python pip
apt-get update
apt-get install -y python3-pip

# Install Python libraries
pip3 install pymysql pymongo pandas direct_redis
