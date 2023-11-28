#!/bin/bash

# Update the package index
sudo apt-get update

# Install Python3, Pip and Redis
sudo apt-get install -y python3 python3-pip redis-server

# Install the required Python libraries
pip3 install pymysql pandas direct_redis
