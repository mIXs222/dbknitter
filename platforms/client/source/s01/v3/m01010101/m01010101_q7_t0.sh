#!/bin/bash

# Update packages
sudo apt-get update -y

# Install pip and Python dev
sudo apt-get install -y python3-pip python3-dev

# Install pandas
pip3 install pandas

# Install mysql.connector
pip3 install mysql-connector-python

# Install pymongo
pip3 install pymongo

# Install dnspython (required for pymongo to connect to MongoDB)
pip3 install dnspython
