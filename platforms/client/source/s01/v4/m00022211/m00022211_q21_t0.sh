#!/bin/bash

# Update package information
apt-get update

# Install Python 3 and PIP if they are not installed
apt-get install -y python3 python3-pip

# Install Python libraries
pip3 install pymysql pymongo direct-redis pandas
