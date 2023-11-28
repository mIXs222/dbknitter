#!/bin/bash
# Update package lists
apt-get update

# Install Python and pip
apt-get install -y python3 python3-pip

# Install required Python libraries
pip3 install pymysql pymongo pandas direct_redis
