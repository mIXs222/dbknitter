#!/bin/bash

# Update system and get package information from all configured sources
sudo apt-get update

# Install pip for Python3
sudo apt-get install -y python3-pip

# Install MongoDB driver for Python
pip3 install pymongo

# Install Pandas for data manipulation
pip3 install pandas

# Install Redis extension for pandas dataframe read
pip3 install direct_redis

# Install additional Python3 package for Redis data format
pip3 install msgpack-python
