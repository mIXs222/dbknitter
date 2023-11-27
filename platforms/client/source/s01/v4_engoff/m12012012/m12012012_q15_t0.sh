#!/bin/bash

# install_dependencies.sh

# Update the package list
sudo apt-get update

# Install pip if it's not already installed
sudo apt-get install -y python3-pip

# Install the required Python packages
pip3 install pandas pymongo direct-redis datetime

# Run the Python script (optional, can be run separately)
# python3 top_supplier_query.py
