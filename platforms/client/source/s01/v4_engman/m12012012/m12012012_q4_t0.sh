#!/bin/bash

# setup.sh

# Update the package list
sudo apt-get update

# Install Python 3 and pip if they are not installed
sudo apt-get install -y python3 python3-pip

# Install pymongo and direct_redis via pip
pip3 install pymongo direct-redis

# Install pandas via pip
pip3 install pandas
