#!/bin/bash

# Update package lists
apt-get update

# Install pip and Redis, if they are not already installed
apt-get install -y python3-pip redis-server

# Install Python libraries
pip3 install pymysql pandas direct_redis
