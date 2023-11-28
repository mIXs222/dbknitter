#!/bin/bash
# Bash script to install all dependencies for the Python code

# Update package lists
apt-get update

# Install Python and Pip if they are not installed
apt-get install -y python3 python3-pip

# Install the required Python libraries
pip3 install pymysql pymongo pandas direct-redis
