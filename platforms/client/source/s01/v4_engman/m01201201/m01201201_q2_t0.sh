#!/bin/bash

# Update package list
apt-get update

# Install pip if not already installed
apt-get install -y python3-pip

# Install required Python packages
pip install pandas pymysql pymongo direct_redis
