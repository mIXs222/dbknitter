#!/bin/bash

# install_dependencies.sh

# Update package list
sudo apt-get update

# Install Python3 and Pip
sudo apt-get install -y python3 python3-pip

# Install requirement packages
pip3 install pymongo pandas direct-redis
