#!/bin/bash

# Update and install system packages
sudo apt-get update -y
sudo apt-get install -y python3-pip python3-dev

# Install Python dependencies
pip3 install pymysql pymongo pandas direct-redis
