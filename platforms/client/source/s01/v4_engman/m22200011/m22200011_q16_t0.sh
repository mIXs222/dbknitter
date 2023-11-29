#!/bin/bash
# Bash script to install all dependencies for the python code

# Update package list
sudo apt-get update

# Install Python 3 and Pip
sudo apt-get install -y python3 python3-pip

# Install pymysql for MySQL database connection
pip3 install pymysql

# Install redis for Redis database connection
pip3 install redis

# Install pandas for data manipulation
pip3 install pandas
