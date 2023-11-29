#!/bin/bash

# Update package lists
apt-get update

# Install pip if not already installed
apt-get install -y python3-pip

# Install Python libraries required
pip3 install pymysql pymongo
