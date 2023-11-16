#!/bin/bash

# install_dependencies.sh
# Activate your virtual environment if needed before running this script

# Update package index (Ubuntu/Debian example)
sudo apt update

# Install Python 3 and pip if not already installed
sudo apt install python3 python3-pip

# Install the pymongo package
pip3 install pymongo
