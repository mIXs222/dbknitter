#!/bin/bash

# Update packages and install Python 3 and pip
sudo apt-get update
sudo apt-get install -y python3 python3-pip

# Install the necessary Python packages
pip3 install pymysql pandas redis direct_redis
