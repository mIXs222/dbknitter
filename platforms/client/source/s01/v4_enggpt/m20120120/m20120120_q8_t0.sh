#!/bin/bash

# Update package lists
sudo apt-get update

# Install Python and pip if needed
sudo apt-get install -y python3 python3-pip

# Install PyMySQL
pip3 install pymysql

# Install PyMongo
pip3 install pymongo

# Install pandas
pip3 install pandas

# Install direct_redis
pip3 install direct_redis
