#!/bin/bash

# Update the package index
sudo apt-get update

# Install Python3 and Pip if they are not already installed
sudo apt-get install -y python3 python3-pip

# Install Python libraries
pip3 install pymysql pymongo pandas direct_redis
