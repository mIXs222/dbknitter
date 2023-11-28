#!/bin/bash

# Update package list
apt-get update -y

# Install Python and pip
apt-get install -y python3 python3-pip

# Install Python libraries
pip3 install pymysql pymongo pandas direct-redis

# Set Python3 as the default python interpreter
ln -sf /usr/bin/python3 /usr/bin/python
