#!/bin/bash

# Update package list
apt-get update

# Install MySQL client
apt-get install -y default-mysql-client

# Install python3, pip and necessary dependencies
apt-get install -y python3 python3-pip

# Upgrade pip to the latest version
pip3 install --upgrade pip

# Install Python libraries
pip3 install pymysql pymongo pandas redis direct_redis
