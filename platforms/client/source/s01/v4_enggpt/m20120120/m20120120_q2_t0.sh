#!/bin/bash

# Update package lists
apt-get update

# Install pip for Python 3 if not already installed
apt-get install -y python3-pip

# Install MySQL and MongoDB clients, Redis tools, and the libzmq3-dev library
apt-get install -y default-libmysqlclient-dev libzmq3-dev

# Install the necessary Python libraries
pip3 install pymysql pymongo pandas redis direct_redis
