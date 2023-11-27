#!/bin/bash

# Update package lists
echo "Updating package lists..."
sudo apt-get update

# Install Python 3 and pip if they are not already installed
echo "Installing Python3 and pip..."
sudo apt-get install -y python3 python3-pip

# Install Python MySQL driver
echo "Installing pymysql..."
pip3 install pymysql

# Install Python MongoDB driver
echo "Installing pymongo..."
pip3 install pymongo
