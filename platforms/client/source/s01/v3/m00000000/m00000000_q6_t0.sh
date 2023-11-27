#!/bin/bash

# Update the package lists
sudo apt-get update

# Install pip for Python3
sudo apt-get install -y python3-pip

# Install necessary Python packages
pip3 install mysql-connector-python
pip3 install pymongo
