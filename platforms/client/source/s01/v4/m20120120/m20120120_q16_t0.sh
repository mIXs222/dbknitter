#!/bin/bash

# Update and install system packages
sudo apt-get update
sudo apt-get install -y python3 python3-pip

# Install Python libraries
pip3 install pymysql pymongo pandas redis
