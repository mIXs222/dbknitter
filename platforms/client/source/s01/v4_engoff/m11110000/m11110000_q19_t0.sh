#!/bin/bash

# Update package lists
apt-get update

# Install pip and Python dev tools
apt-get install -y python3-pip python3-dev

# Install pymysql and pymongo
pip3 install pymysql pymongo
