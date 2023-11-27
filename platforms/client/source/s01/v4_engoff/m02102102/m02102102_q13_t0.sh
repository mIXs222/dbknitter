#!/bin/bash

# Update package list
apt-get update

# Upgrade existing packages
apt-get upgrade -y

# Install Python 3 and pip
apt-get install -y python3 python3-pip

# Install Python MySQL client
pip3 install pymysql

# Install Python MongoDB client
pip3 install pymongo
