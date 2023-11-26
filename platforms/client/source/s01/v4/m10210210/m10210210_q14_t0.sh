#!/bin/bash

# Install Python3 and pip if they are not installed
sudo apt-get update
sudo apt-get install -y python3 python3-pip

# Install Python library pymysql for MySQL database connection
pip3 install pymysql

# Install sqlalchemy to deal with database relations in Python
pip3 install sqlalchemy

# Install direct_redis for Redis connection
pip3 install direct_redis

# Install pandas for handling dataframes
pip3 install pandas
