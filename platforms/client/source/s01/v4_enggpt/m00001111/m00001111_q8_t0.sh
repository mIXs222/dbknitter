#!/bin/bash

# Update package lists
apt-get update

# Install pip if it's not already installed
apt-get install -y python3-pip

# Install Python MySQL driver
pip3 install pymysql

# Install Python MongoDB driver
pip3 install pymongo
