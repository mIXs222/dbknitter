#!/bin/bash

# Update package lists
sudo apt-get update

# Install Python3 Pip if not already installed
sudo apt-get install -y python3-pip

# Install required Python libraries
pip3 install pandas pymysql pymongo direct-redis
