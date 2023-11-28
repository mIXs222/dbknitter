#!/bin/bash

# Update package list and install pip if it's not available
sudo apt-get update
sudo apt-get install -y python3-pip

# Install Python libraries pymysql, pymongo, pandas, and direct_redis
pip3 install pymysql pymongo pandas direct_redis
