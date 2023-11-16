#!/bin/bash

# Update package lists
apt-get update

# Install pip for Python 3 if not installed
apt-get install -y python3-pip

# Install the required Python packages
pip3 install pymysql direct_redis pandas
