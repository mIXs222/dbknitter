#!/bin/bash

# Update package lists
sudo apt-get update

# Install Python 3 and pip if they are not installed
sudo apt-get install -y python3 python3-pip

# Install the required Python libraries
pip3 install pymysql pandas redis

# Install direct_redis from GitHub
pip3 install git+https://github.com/NoneGG/direct_redis.git

# Make sure to install other dependencies, if needed
