#!/bin/bash

# Update package lists
sudo apt-get update

# Install pip for Python 3
sudo apt-get install -y python3-pip

# Install Python Redis library
pip3 install redis pandas

# Install pymysql for connecting to MySQL
pip3 install pymysql

# Install Direct Redis library
pip3 install direct_redis

# Note: Assure to give executable permissions to the bash script before running:
# chmod +x setup.sh
