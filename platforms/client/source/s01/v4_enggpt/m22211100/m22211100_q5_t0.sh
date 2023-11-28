#!/bin/bash

# Update package list
apt-get update

# Install Python3, pip and Redis
apt-get install -y python3 python3-pip redis-server

# Install Python libraries
pip3 install pymysql pymongo pandas direct_redis
