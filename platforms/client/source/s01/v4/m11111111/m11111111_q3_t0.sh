#!/bin/bash

# Update package lists
sudo apt-get update

# Install MongoDB
sudo apt-get install -y mongodb

# Install Python3 and PIP
sudo apt-get install -y python3 python3-pip

# Install python packages
pip3 install pymongo pandas
