#!/bin/bash

# Update package list
apt-get update

# Install pip for Python package management
apt-get install -y python3-pip

# Install Python MySQL client
pip3 install pymysql

# Install Pandas library
pip3 install pandas

# Install redis-py library
pip3 install redis direct-redis
