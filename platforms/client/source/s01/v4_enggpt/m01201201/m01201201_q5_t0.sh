#!/bin/bash

# Update repositories and install pip
sudo apt-get update
sudo apt-get install -y python3-pip

# Install Python packages
pip3 install pymysql pymongo pandas direct-redis
