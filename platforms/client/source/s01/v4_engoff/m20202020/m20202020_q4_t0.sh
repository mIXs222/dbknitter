#!/bin/bash

# Update package lists
sudo apt-get update

# Install pip for Python3
sudo apt-get install -y python3-pip

# Install Python libraries required for the script
pip3 install pymysql pandas direct-redis
