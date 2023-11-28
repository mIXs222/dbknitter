#!/bin/bash

# Update package manager and install Python3, pip, and redis if necessary
sudo apt update
sudo apt install -y python3 python3-pip

# Install required Python libraries
pip3 install pymysql pandas pymongo direct_redis
