#!/bin/bash

# Update package lists
apt-get update

# Install Python and pip
apt-get install -y python3 python3-pip

# Install PyMySQL and PyMongo using pip
pip3 install pymysql pymongo
