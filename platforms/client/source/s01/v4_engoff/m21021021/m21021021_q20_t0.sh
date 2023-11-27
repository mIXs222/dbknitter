#!/bin/bash

# Update package list
apt-get update -y

# Install pip if it's not installed
apt-get install -y python3-pip

# Install Python libraries
pip3 install pymysql pymongo pandas direct_redis
