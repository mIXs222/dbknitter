#!/bin/bash

# Update package index
apt-get update

# Install pip if not already installed
apt-get install -y python3-pip

# Install Python MySQL library (pymysql)
pip3 install pymysql

# Install Python MongoDB library (pymongo)
pip3 install pymongo
