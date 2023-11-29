#!/bin/bash

# Update package info
apt-get update

# Install Python, pip, and Python MySQL client
apt-get install -y python3 python3-pip default-libmysqlclient-dev

# Install Redis and associated tools
apt-get install -y redis-server

# Install Python libraries
pip3 install pymysql pandas redis msgpack-python direct_redis
