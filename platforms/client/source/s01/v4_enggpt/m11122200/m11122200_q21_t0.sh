#!/bin/bash

# Update package lists
apt-get update

# Install Python and Pip if they are not installed
apt-get install -y python3 python3-pip

# Install Python libraries needed for the script
pip3 install pandas pymysql pymongo direct_redis
