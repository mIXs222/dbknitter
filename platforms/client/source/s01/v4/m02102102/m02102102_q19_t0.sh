#!/bin/bash

# install_dependencies.sh

# Update packages list and install Python 3 and pip
sudo apt-get update
sudo apt-get install -y python3 python3-pip

# Install Python packages for MongoDB, Redis, and pandas
pip3 install pymongo direct_redis pandas
