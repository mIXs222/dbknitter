#!/bin/bash

# Update package index
sudo apt-get update

# Install MySQL client
sudo apt-get install -y default-mysql-client

# Install Redis client
sudo apt-get install -y redis-tools

# Install Python 3 and pip
sudo apt-get install -y python3 python3-pip

# Install Python dependencies
pip3 install pymysql pandas direct-redis
