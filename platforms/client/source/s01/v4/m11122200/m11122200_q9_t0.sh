#!/bin/bash

# Update package list
apt update

# Install Python and Pip if they are not installed
apt install -y python3
apt install -y python3-pip

# Install required Python packages
pip3 install pandas pymysql pymongo direct-redis
