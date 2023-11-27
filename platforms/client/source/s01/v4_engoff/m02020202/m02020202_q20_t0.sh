#!/bin/bash

# Update and install system packages
sudo apt-get update
sudo apt-get install -y python3 python3-pip

# Install necessary Python packages
pip3 install pymysql pandas redis direct_redis
