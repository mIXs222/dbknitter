#!/bin/bash
# Installing Python 3.x and Python package manager (pip)
apt-get update
apt-get install -y python3 python3-pip

# Install required Python libraries
pip3 install pymysql
pip3 install pymongo
pip3 install pandas
pip3 install git+https://github.com/antirez/redis-py.git@master#egg=direct_redis
