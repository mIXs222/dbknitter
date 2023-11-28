#!/bin/bash

# Update package lists
apt-get update

# Install MySQL client (might not be necessary on some systems)
apt-get install -y default-mysql-client

# Install Redis-tools (might not be necessary on some systems)
apt-get install -y redis-tools

# Install Python3 and Pip
apt-get install -y python3 python3-pip

# Install required Python libraries
pip3 install pymysql pymongo pandas
