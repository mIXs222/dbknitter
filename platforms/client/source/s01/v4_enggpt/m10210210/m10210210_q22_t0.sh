#!/bin/bash

# Update package lists
sudo apt-get update

# Install python3 if not installed
sudo apt-get install -y python3

# Install pip if not installed
sudo apt-get install -y python3-pip

# Install pymongo
pip3 install pymongo

# Install pandas
pip3 install pandas

# Install direct_redis
pip3 install git+https://github.com/mymarilyn/direct_redis.git
