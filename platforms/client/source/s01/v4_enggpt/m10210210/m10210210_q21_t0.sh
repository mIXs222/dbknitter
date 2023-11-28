#!/bin/bash

# Update package list
sudo apt-get update

# Install Python 3 and pip if not present
sudo apt-get install -y python3 python3-pip

# Install PyMySQL
pip3 install pymysql

# Install PyMongo
pip3 install pymongo
