#!/bin/bash

# Update package lists
sudo apt-get update

# Install Python and pip
sudo apt-get install -y python3 python3-pip

# Install required Python libraries
pip3 install pymysql pandas redis direct_redis
