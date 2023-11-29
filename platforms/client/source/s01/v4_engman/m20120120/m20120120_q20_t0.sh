#!/bin/bash

# Update repositories and install Python 3 and pip
sudo apt update
sudo apt install -y python3 python3-pip

# Install Python library dependencies
pip3 install pymysql pandas pymongo direct_redis
