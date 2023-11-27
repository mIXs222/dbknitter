#!/bin/bash

# Update the package lists for upgrades for packages that need upgrading
sudo apt-get update

# Python3 pip install 
sudo apt-get install -y python3-pip

# Install mysql connector for Python
pip3 install mysql-connector-python

# Install pymongo for Python
pip3 install pymongo
