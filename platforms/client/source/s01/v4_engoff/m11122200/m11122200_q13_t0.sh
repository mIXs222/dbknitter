#!/bin/bash
# File: setup.sh

# Update package list and install pip
apt-get update
apt-get install -y python3-pip

# Install the required Python packages
pip3 install pandas pymysql direct-redis
