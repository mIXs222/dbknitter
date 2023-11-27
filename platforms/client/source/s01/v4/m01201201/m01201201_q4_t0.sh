#!/bin/bash

# Update package list
apt-get update

# Install Python package manager pip
apt-get install -y python3-pip

# Install required Python packages
pip3 install pymysql
pip3 install pymongo
