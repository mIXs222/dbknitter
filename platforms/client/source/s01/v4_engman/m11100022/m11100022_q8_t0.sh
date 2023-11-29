#!/bin/bash

# Bash script to install dependencies

# Update package lists
apt-get update

# Install pip if not present
apt-get install -y python3-pip

# Install Python libraries
pip3 install pymysql pymongo pandas direct-redis
