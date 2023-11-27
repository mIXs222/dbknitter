#!/bin/bash

# Update package list
sudo apt-get update

# Install Python 3 and pip if they are not installed
sudo apt-get install -y python3 python3-pip

# Install the MySQL client and development libraries
sudo apt-get install -y default-libmysqlclient-dev

# Install the required Python packages
pip3 install pymysql pymongo pandas direct-redis
