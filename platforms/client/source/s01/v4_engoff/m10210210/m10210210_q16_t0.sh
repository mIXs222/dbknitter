#!/bin/bash

# Update package list
apt-get update

# Install Python and pip if not installed
apt-get install -y python3 python3-pip

# Install required Python packages
pip3 install pymysql pymongo pandas direct-redis
