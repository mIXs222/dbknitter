#!/bin/bash

# Update package lists
sudo apt-get update

# Install Python3 and pip if they are not installed
sudo apt-get install -y python3 python3-pip

# Install pymysql and pandas
pip3 install pymysql pandas

# Assuming direct_redis is a custom library, you might need to install it in a different way
# If direct_redis is a package available on PyPI, you could uncomment the following line:
#pip3 install direct_redis
