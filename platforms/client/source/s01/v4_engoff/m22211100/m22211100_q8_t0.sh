#!/bin/bash

# Update package list
sudo apt-get update

# Install Python3 and Pip if not installed
sudo apt-get install -y python3 python3-pip

# Install MySQL client
sudo apt-get install -y default-libmysqlclient-dev

# Install Redis
sudo apt-get install -y redis-server

# Install Python packages
pip3 install pymysql
pip3 install pymongo
pip3 install pandas
pip3 install git+https://github.com/alexburlacu/direct_redis.git
