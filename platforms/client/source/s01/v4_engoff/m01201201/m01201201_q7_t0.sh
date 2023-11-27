#!/bin/bash

# Update package list and install pip if not already installed
sudo apt-get update
sudo apt-get install -y python3-pip

# Install required Python libraries
pip3 install pymysql pymongo pandas direct_redis
