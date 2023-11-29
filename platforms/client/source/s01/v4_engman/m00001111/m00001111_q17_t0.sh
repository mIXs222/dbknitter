#!/bin/bash

# Update package lists
sudo apt-get update

# Install pip for python3 if not installed
sudo apt-get install -y python3-pip

# Install Python MySQL driver (pymysql)
sudo pip3 install pymysql

# Install Python MongoDB driver (pymongo)
sudo pip3 install pymongo
