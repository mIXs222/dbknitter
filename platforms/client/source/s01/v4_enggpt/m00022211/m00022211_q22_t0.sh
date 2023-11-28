#!/bin/bash

# Updating package list and upgrading packages
apt-get update -y
apt-get upgrade -y

# Installing Python 3 and Pip
apt-get install python3 -y
apt-get install python3-pip -y

# Installing required Python packages
pip3 install pymongo
pip3 install pandas
pip3 install git+https://github.com/bluele/direct_redis.git  # Assuming direct_redis is not on PyPI
