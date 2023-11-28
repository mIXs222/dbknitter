#!/bin/bash

# Update package lists
apt-get update

# Install Python 3 and pip if they are not installed
apt-get install -y python3 python3-pip

# Install 'pymysql' library for MySQL connections
pip3 install pymysql

# Install 'pymongo' library for MongoDB connections
pip3 install pymongo

# Give execute permission to the Python script
chmod +x execute_query.py
