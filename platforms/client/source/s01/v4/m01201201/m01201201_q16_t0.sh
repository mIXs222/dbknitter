#!/bin/bash

# Install system-level dependencies
sudo apt-get update
sudo apt-get install -y python3 python3-pip libmysqlclient-dev

# Install Python package dependencies
pip3 install pymysql pymongo pandas direct_redis
