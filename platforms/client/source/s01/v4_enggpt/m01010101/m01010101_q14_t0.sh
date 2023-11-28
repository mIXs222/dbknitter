#!/bin/bash
# Bash script to install dependencies

# Update package lists
apt-get update

# Install Python and Pip if they are not installed
apt-get install -y python3
apt-get install -y python3-pip

# Install required Python packages
python3 -m pip install pymysql pymongo
