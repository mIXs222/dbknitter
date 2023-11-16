#!/bin/bash

# Update package lists
sudo apt-get -y update

# Install Python3
sudo apt-get install -y python3

# Install pip for Python3
sudo apt-get install -y python3-pip

# Install pymongo, pandas libraries
pip3 install pymongo pandas
