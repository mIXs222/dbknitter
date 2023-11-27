#!/bin/bash

# Update package list
sudo apt-get update

# Install Python3, pip and required system libraries
sudo apt-get install -y python3 python3-pip

# Install pymysql and pandas
pip3 install pymysql pandas

# Install Redis client for direct_redis module
sudo apt-get install -y redis

# Install direct_redis
pip3 install git+https://github.com/agoragames/direct_redis.git
