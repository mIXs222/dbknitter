#!/bin/bash

# Update package list
sudo apt-get update

# Install Python3 and pip if they are not installed
sudo apt-get install python3 python3-pip -y

# Install pymongo and pandas
pip3 install pymongo pandas

# Install direct_redis (you may need to install its dependencies or find the correct package via pip)
pip3 install git+https://github.com/direct-redis/direct-redis.git
