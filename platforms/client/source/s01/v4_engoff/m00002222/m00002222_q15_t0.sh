#!/bin/bash

# Update package list and install pip if not installed
sudo apt-get update
sudo apt-get install -y python3-pip

# Install pymysql and pandas
pip3 install pymysql pandas

# Install direct_redis
pip3 install git+https://github.com/RedisJSON/direct_redis
