#!/bin/bash

# Update package list
apt-get update

# Install Python and pip
apt-get install -y python3 python3-pip

# Install MongoDB driver
pip3 install pymongo

# Install Redis driver for DataFrame
pip3 install git+https://github.com/RedisJSON/redisjson-py.git

# Install pandas library
pip3 install pandas
