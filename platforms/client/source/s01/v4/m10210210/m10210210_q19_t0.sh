#!/bin/bash

# Install Python3 and pip3 if not installed
sudo apt-get update
sudo apt-get install -y python3 python3-pip

# Install required Python packages
pip3 install pymysql pandas direct_redis
