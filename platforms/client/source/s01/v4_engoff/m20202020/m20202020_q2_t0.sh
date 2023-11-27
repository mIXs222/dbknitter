#!/bin/bash

# Update the package list
sudo apt-get update

# Install pip for Python3 and Redis server
sudo apt-get install -y python3-pip redis-server

# Install pymysql and pandas through pip
pip3 install pymysql pandas

# Assuming direct_redis is a custom module available in the system or it's provided as a Python script,
# otherwise, the install command would fail.
pip3 install direct_redis
