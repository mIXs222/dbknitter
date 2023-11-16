#!/bin/bash

# Bash script to install all dependencies for the Python code

# Update package lists
sudo apt-get update

# Install pip for Python 3
sudo apt-get install -y python3-pip

# Install MySQL development headers and libraries to compile the MySQL client
sudo apt-get install -y default-libmysqlclient-dev

# Install Redis server
sudo apt-get install -y redis-server

# Install required Python libraries
pip3 install pymysql pandas sqlalchemy direct_redis
