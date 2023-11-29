#!/bin/bash

# Update package list
apt-get update

# Install pip if not installed
apt-get install -y python3-pip

# Install Python MySQL client
pip3 install pymysql

# Install Python MongoDB client
pip3 install pymongo
