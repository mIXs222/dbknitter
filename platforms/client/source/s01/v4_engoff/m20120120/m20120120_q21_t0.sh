#!/bin/bash

# install_dependencies.sh

# Update package lists
sudo apt-get update

# Install Python pip if it's not already available
sudo apt-get install -y python3-pip

# Install MySQL driver
pip install pymysql

# Install Pandas for data manipulation
pip install pandas

# Install direct_redis for Redis connection
pip install direct_redis
