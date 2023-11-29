#!/bin/bash

# Update packages list
sudo apt-get update

# Install Python and Pip
sudo apt-get install -y python3 python3-pip

# Install MongoDB driver
pip3 install pymongo

# Install Redis package for Python
pip3 install direct-redis

# Install Pandas
pip3 install pandas
