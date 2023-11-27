#!/bin/bash

# Updates package lists for upgrades and new package installations
sudo apt-get update -y

# Install Python 3 and PIP if they are not already installed
sudo apt-get install -y python3 python3-pip

# Install required Python libraries
pip3 install pymysql pymongo pandas direct_redis
