#!/bin/bash

# Update package list and upgrade the system
sudo apt-get update && sudo apt-get upgrade -y

# Install pip for Python3
sudo apt-get install python3-pip -y

# Install the pymongo package
pip3 install pymongo

# Install pandas for data manipulation
pip3 install pandas

# Install direct_redis for Redis connection
pip3 install git+https://github.com/amyangfei/direct_redis.git
