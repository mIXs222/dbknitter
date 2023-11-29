#!/bin/bash

# Update repositories and upgrade packages
apt-get update
apt-get -y upgrade

# Install Python and pip
apt-get install -y python3 python3-pip

# Install the required Python libraries
pip3 install pymysql pandas direct_redis

# Set the script to be executable
chmod +x top_supplier.py

# Run the script (optional, can be executed manually)
# python3 top_supplier.py
