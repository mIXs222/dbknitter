#!/bin/bash

# Update package list and install pip
sudo apt update
sudo apt install -y python3-pip

# Install required Python libraries
pip3 install pymysql pymongo pandas direct_redis
