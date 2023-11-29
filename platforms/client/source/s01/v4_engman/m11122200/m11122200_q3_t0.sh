#!/bin/bash
set -e

# Update package list
sudo apt-get update

# Install MySQL client and Redis
sudo apt-get install -y default-libmysqlclient-dev redis

# Install Python and pip
sudo apt-get install -y python3-pip

# Install Python libraries
pip3 install pymysql pandas csv direct_redis
