#!/bin/bash
# This script will install all necessary dependencies for the python script

# Update the package lists for upgrades for packages that need upgrading
sudo apt-get update

# Install python
sudo apt-get install python3.6

# Install pip
sudo apt-get install python3-pip

# Install mysql.connector
pip3 install mysql-connector-python-rf

# Install pymongo
pip3 install pymongo
