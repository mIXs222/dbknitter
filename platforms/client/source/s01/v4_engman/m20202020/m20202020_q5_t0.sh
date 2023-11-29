#!/bin/bash

# Update packages list
sudo apt-get update

# Install pip for Python3
sudo apt-get install -y python3-pip

# Install MySQL dependencies
sudo apt-get install -y default-libmysqlclient-dev build-essential

# Install Redis server and direct_redis dependencies
sudo apt-get install -y redis-server

# Install the necessary Python packages
pip3 install pymysql pandas redis direct_redis
