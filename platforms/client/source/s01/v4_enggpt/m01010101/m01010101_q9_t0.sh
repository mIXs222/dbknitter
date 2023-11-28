#!/bin/bash

# Update package lists
apt-get update

# Install Python pip if it's not already installed
apt-get install -y python3-pip

# Install MySQL and MongoDB client libraries for Python
pip3 install pymysql pymongo
