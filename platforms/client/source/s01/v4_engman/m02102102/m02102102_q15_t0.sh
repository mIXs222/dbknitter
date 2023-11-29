#!/bin/bash

# Update package lists
sudo apt-get update

# Install Python 3 and pip
sudo apt-get install -y python3 python3-pip

# Install pymysql
pip3 install pymysql

# Install pandas
pip3 install pandas

# Install custom direct_redis (Assuming the installation process is similar to redis-py)
pip3 install git+https://github.com/your_repository/direct_redis.git
