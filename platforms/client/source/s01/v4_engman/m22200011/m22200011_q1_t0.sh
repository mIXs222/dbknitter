#!/bin/bash
# install_dependencies.sh

# Update the package list
apt-get update

# Install Python3 and PIP if they are not already installed
apt-get install -y python3 python3-pip

# Install pymongo using pip
pip3 install pymongo
