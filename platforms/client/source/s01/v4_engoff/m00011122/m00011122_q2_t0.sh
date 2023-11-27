#!/bin/bash

# This script installs all the necessary dependencies to run the Python code.

# Update package lists
sudo apt-get update

# Install MySQL client if needed
which mysql || sudo apt-get install -y mysql-client

# Install pip if not installed
which pip || sudo apt-get install -y python3-pip

# Install Python MySQL driver
pip install pymysql

# Install Python MongoDB driver
pip install pymongo
