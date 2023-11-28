#!/bin/bash

# Update package lists
sudo apt-get update

# Install Python3 and pip if they are not installed
sudo apt-get install -y python3 python3-pip

# Install pymongo for Python3
pip3 install pymongo
