#!/bin/bash
# Bash script to install all dependencies for the python script

# Update package list
sudo apt-get update

# Install Python and pip if they are not installed
sudo apt-get install -y python3 python3-pip

# Install pymysql
pip3 install pymysql

# Install pymongo
pip3 install pymongo

# Install pandas
pip3 install pandas

# Since 'direct_redis' is not a standard package and doesn't exist as of the knowledge cutoff date (2023)
# Assuming it is a custom or local package that needs to be installed separately, 
# appropriate steps would be needed to install this package.
