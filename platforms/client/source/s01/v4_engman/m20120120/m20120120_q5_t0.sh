#!/bin/bash

# Update and install system packages
sudo apt-get update
sudo apt-get install -y python3-pip

# Install required python packages
pip3 install pymysql pymongo pandas direct-redis
