#!/bin/bash

# You may need to run this script with sudo privileges

# Update repositories and install Python 3 and pip if they are not installed
apt-get update
apt-get install -y python3 python3-pip

# Install Python dependencies
pip3 install pymysql pymongo
