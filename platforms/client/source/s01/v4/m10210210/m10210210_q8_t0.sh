#!/bin/bash

# Update package list
apt-get update

# Install pip for Python package management
apt-get install -y python3-pip

# Install MySQL client libraries
apt-get install -y default-libmysqlclient-dev

# Install python packages
pip3 install pandas pymysql pymongo direct-redis
