#!/bin/bash

# install_dependencies.sh

# Update package list
sudo apt-get update

# Install Python and pip if necessary
sudo apt-get install -y python3
sudo apt-get install -y python3-pip

# Install pymongo
pip3 install pymongo
