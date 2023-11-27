#!/bin/bash

# Update package lists
sudo apt-get update

# Install Python3 and pip if they are not installed
sudo apt-get install -y python3 python3-pip

# Install Python MySQL client (pymysql)
pip3 install pymysql

# Install Python MongoDB client (pymongo)
pip3 install pymongo
