#!/bin/bash

# Update package list
apt-get update

# Install Python and Pip
apt-get install -y python3 python3-pip

# Install required Python libraries
pip3 install pymysql pymongo pandas direct_redis
