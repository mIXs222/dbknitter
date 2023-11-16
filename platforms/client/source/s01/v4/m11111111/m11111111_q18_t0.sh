#!/bin/bash

# Update package lists
apt-get update

# Install Python3 and Pip
apt-get install -y python3 python3-pip

# Install MongoDB driver
pip3 install pymongo

# Install pandas
pip3 install pandas

# Run the python script
python3 query.py
