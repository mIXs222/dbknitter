#!/bin/bash

# Update package lists
sudo apt-get update

# Install Python3 and pip, if not already installed
sudo apt-get install python3 python3-pip -y

# Install the required Python packages
pip3 install pandas pymysql pymongo direct-redis
