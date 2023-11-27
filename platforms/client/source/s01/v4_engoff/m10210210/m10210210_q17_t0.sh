#!/bin/bash

# Update package listings
sudo apt-get update

# Install Python 3 and PIP
sudo apt-get install -y python3 python3-pip

# Install Python packages (pymysql, pandas, direct_redis)
pip3 install pymysql pandas direct_redis
