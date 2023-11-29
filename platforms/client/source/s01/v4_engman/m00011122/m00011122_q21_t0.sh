#!/bin/bash

# Update package manager and install pip for Python
sudo apt-get update
sudo apt-get install -y python3-pip

# Install required Python libraries
pip3 install pymysql pymongo pandas redis direct-redis
