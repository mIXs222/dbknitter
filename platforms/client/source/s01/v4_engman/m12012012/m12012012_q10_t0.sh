#!/bin/bash

# Update package list
sudo apt-get update

# Install Python and pip
sudo apt-get install -y python3 python3-pip

# Install required libraries
pip3 install pymysql pymongo pandas direct_redis
