#!/bin/bash

# Update package lists
apt-get update

# Install pip (or make sure it's up to date)
apt-get install -y python3-pip

# Install necessary libraries
pip3 install pymysql pymongo pandas direct-redis
