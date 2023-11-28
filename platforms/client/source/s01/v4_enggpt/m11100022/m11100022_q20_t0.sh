#!/bin/bash

# Update package lists
sudo apt-get update

# Install Python 3 and pip if not already installed
sudo apt-get install -y python3 python3-pip

# Install required Python libraries
pip3 install pymysql pymongo pandas direct-redis
