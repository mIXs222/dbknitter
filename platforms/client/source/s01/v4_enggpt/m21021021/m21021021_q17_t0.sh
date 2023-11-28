#!/bin/bash

# Update the package list
apt-get update

# Install pip (if it's not already installed)
apt-get install -y python3-pip

# Install the Python MySQL driver
pip3 install pymysql

# Install the Python MongoDB driver
pip3 install pymongo
