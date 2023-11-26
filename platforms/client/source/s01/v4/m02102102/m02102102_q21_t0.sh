#!/bin/bash

# Make sure to update the system's package index
sudo apt-get update

# Install Python and pip if they're not installed
sudo apt-get install -y python3 python3-pip

# Install pymysql and pandas using pip
pip3 install pymysql pandas

# Install direct_redis and its dependencies
pip3 install git+https://github.com/nakagami/direct_redis.git

# Remind user to start the python script
echo "Dependencies are installed. Run the python script with 'python3 query_code.py'."
