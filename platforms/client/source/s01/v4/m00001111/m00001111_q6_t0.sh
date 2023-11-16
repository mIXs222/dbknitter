#!/bin/bash

# Activate virtual environment if you are using one
# source /path/to/your/virtualenv/bin/activate

# Update the package list
sudo apt-get update

# Install pip if it is not installed
sudo apt-get install -y python3-pip

# Install pymongo
pip3 install pymongo
