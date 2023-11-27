#!/bin/bash

# Update package manager and get pip if not already installed
sudo apt-get update
sudo apt-get install -y python3-pip

# Install PyMySQL
pip3 install pymysql

# Install PyMongo
pip3 install pymongo
