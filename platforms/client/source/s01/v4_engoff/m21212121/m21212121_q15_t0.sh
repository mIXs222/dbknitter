#!/bin/bash

# install_dependencies.sh

# Update package list
sudo apt-get update

# Install Python and pip if they are not installed
sudo apt-get install -y python3 python3-pip

# Install pymongo using pip
pip3 install pymongo
