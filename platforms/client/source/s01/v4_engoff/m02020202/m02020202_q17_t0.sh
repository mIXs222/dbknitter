#!/bin/bash

# Update package list
sudo apt-get update

# Install Python3 and pip if they are not already installed
sudo apt-get install -y python3 python3-pip

# Install Python package for MySQL
pip3 install pymysql

# Install pandas package for data manipulation
pip3 install pandas

# Install direct_redis package for Redis data retrieval
pip3 install git+https://github.com/predict-idlab/direct-redis.git
