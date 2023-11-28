#!/bin/bash

# Update package information
apt-get update -y

# Install Python and pip
apt-get install -y python3
apt-get install -y python3-pip

# Install Python packages
pip3 install pymysql pymongo
