#!/bin/bash
# install.sh

# Update package list
sudo apt-get update

# Install pip for Python if not already installed
sudo apt-get install -y python3-pip

# Install MySQL client, if needed
sudo apt-get install -y default-libmysqlclient-dev

# Install Python libraries
pip3 install pymysql pandas direct_redis
