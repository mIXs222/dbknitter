#!/bin/bash
# Install Python and pip
sudo apt-get update
sudo apt-get install -y python3 python3-pip

# Install pymysql and pandas libraries
pip3 install pymysql pandas

# Install direct_redis (assuming it's a valid package or it has to be installed from a repository)
pip3 install direct_redis
