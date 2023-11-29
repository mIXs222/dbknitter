#!/bin/bash
# setup.sh

# Update package list and install pip
sudo apt-get update
sudo apt-get install -y python3 python3-pip

# Install Python dependencies
pip3 install pymysql pandas direct-redis
