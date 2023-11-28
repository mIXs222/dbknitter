#!/bin/bash

# Update the package list
apt-get update

# Install Python3 and pip
apt-get install -y python3 python3-pip

# Install MySQL client
apt-get install -y default-mysql-client

# Install Redis client
apt-get install -y redis-tools

# Install required Python packages
pip3 install pymysql pandas direct-redis
