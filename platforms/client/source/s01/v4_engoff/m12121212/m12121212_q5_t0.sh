#!/bin/bash

# Update package list
apt-get update

# Install pip for Python package installation
apt-get install -y python3-pip

# Install the required Python packages
pip3 install pymongo
pip3 install direct-redis
pip3 install pandas
