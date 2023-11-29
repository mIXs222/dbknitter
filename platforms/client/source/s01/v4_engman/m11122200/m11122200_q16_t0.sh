#!/bin/bash

# Update the package list
sudo apt update

# Install Python pip
sudo apt install -y python3-pip

# Upgrade pip
pip3 install --upgrade pip

# Install the necessary Python packages
pip3 install pymongo
pip3 install pandas
pip3 install git+https://github.com/RedisDirect/direct_redis.git
