#!/bin/bash

# Update package list
apt-get update

# Install pip and Python dev tools
apt-get install python3-pip python3-dev -y

# Upgrade pip
pip3 install --upgrade pip

# Install pymysql
pip3 install pymysql

# Install pandas
pip3 install pandas

# Install direct_redis
pip3 install direct_redis

# Install csv
pip3 install python-csv

# Install datetime
pip3 install datetime
