#!/bin/bash

# Update the package list and install Python pip
apt-get update
apt-get install -y python3-pip

# Install the pymongo package to interact with MongoDB
pip3 install pymongo

# Install the pandas package for data manipulation
pip3 install pandas

# Install the direct_redis package to interact with Redis
pip3 install git+https://github.com/JoshuaBrockschmidt/direct_redis.git
