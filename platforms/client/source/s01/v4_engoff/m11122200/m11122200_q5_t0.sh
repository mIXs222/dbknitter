#!/bin/bash

# Update and install system packages
sudo apt-get update
sudo apt-get install -y python3-pip python3-dev

# Install Python packages
pip3 install pymysql pymongo pandas redis direct-redis
