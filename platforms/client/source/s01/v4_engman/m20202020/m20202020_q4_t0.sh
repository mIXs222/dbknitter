#!/bin/bash

# Update package list
apt-get update

# Install Python3 and pip
apt-get install -y python3 python3-pip

# Direct Redis and its dependencies (redis-py requires to be installed first)
pip3 install redis
pip3 install direct-redis

# Install PyMySQL
pip3 install pymysql

# Install pandas
pip3 install pandas
