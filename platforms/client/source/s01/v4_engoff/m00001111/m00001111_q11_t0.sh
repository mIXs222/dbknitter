#!/bin/bash

# Update package list
apt-get update

# Install Python3, pip and required system libraries for MySQL
apt-get install -y python3 python3-pip default-libmysqlclient-dev build-essential

# Install Python libraries
pip3 install pymysql pymongo
