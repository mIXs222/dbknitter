#!/bin/bash

# Update package list and upgrade existing packages
apt-get update
apt-get -y upgrade

# Install Python 3 and pip (if not already installed)
apt-get install -y python3 python3-pip

# Install MySQL and MongoDB Python clients
pip3 install pymysql
pip3 install pymongo
