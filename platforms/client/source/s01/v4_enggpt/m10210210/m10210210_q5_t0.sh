#!/bin/bash

# Update package list and install Python with pip
sudo apt-get update
sudo apt-get install -y python3 python3-pip

# Install necessary Python libraries
pip3 install pandas pymysql pymongo redis direct_redis
