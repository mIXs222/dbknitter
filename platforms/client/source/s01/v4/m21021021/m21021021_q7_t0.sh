#!/bin/bash
# Update the system's package index
apt-get update

# Install Python and pip
apt-get install -y python3 python3-pip

# Install MySQL dependencies
apt-get install -y default-libmysqlclient-dev gcc

# Install the necessary Python libraries
pip3 install pandas pymysql pymongo direct_redis
