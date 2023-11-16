#!/bin/bash

# Update the package lists
apt-get update

# Install pip
apt-get install -y python3-pip

# Install pandas and pymongo packages for Python
pip3 install pandas pymongo
