#!/bin/bash

# Update package manager and install pip if not present
sudo apt-get update -y
sudo apt-get install -y python3-pip

# Install Python dependencies
pip3 install pymysql pymongo pandas redis direct_redis
