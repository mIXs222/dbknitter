#!/bin/bash

# This script will install Python dependencies required to run the Python script above

# Update the package list
sudo apt-get update

# Install pip if not present
sudo apt-get install -y python3-pip

# Install Python packages
pip3 install pymysql
pip3 install pymongo
pip3 install pandas
pip3 install direct_redis
