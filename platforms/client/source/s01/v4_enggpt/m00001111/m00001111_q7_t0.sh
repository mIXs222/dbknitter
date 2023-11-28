#!/bin/bash

# Update package list
apt-get update

# Install Python and pip (if not already installed)
apt-get install -y python3 python3-pip

# Install the required Python packages
pip3 install pymysql pymongo
