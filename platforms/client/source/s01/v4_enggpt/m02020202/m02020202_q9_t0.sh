#!/bin/bash

# Update package list
sudo apt-get update
sudo apt-get install -y python3-pip

# Install Python libraries
pip3 install pandas
pip3 install pymysql

# Install direct_redis, assuming it exists as a pip package (this may need to be adjusted if it does not)
pip3 install direct_redis
