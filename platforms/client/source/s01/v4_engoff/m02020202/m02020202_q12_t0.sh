#!/bin/bash

# Update the package list
sudo apt-get update

# Install Python3 and pip if they are not installed
sudo apt-get install -y python3 python3-pip

# Install pymysql
pip3 install pymysql

# Install pandas
pip3 install pandas

# Install direct_redis
pip3 install git+https://github.com/RedisGears/direct_redis.git
