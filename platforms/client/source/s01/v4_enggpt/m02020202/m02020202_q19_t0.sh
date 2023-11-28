#!/bin/bash

# Update package list and install pip (if not already installed)
apt-get update
apt-get install -y python3-pip

# Install Python dependencies
pip3 install pandas pymysql direct_redis
