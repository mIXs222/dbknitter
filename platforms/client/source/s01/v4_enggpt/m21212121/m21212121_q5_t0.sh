#!/bin/bash

# Update package list
apt-get update

# Install Python package management system
apt-get install -y python3-pip

# Install MongoDB driver
pip3 install pymongo

# Install Redis driver
pip3 install direct-redis

# Install pandas for data manipulation
pip3 install pandas
