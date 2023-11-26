#!/bin/bash

# Update package lists
apt-get update

# Install Python3 and pip if not already installed
apt-get install -y python3 python3-pip

# Install required Python packages
pip3 install pymysql pymongo
