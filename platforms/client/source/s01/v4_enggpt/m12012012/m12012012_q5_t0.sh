#!/bin/bash

# Update package lists
sudo apt-get update

# Install Python and pip, if not already installed
sudo apt-get install -y python3 python3-pip

# Upgrade pip to the latest version
python3 -m pip install --upgrade pip

# Install the required Python libraries
python3 -m pip install pymysql pymongo pandas direct-redis
