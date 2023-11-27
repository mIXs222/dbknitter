#!/bin/bash

# Update package list
sudo apt-get update

# Install Python 3, pip and required dependencies for MySQL and Redis
sudo apt-get install -y python3 python3-pip
pip3 install pymysql pandas redis direct_redis
