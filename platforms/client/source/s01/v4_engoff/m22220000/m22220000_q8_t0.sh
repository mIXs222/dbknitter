#!/bin/bash
# Install Python, pip and required packages

# Update the package list
apt-get update

# Install Python and pip
apt-get install -y python3 python3-pip

# Install required Python packages
pip3 install pymysql pandas redis direct_redis
