#!/bin/bash

# Update package lists
sudo apt-get update

# Install Python3 and pip if not installed
sudo apt-get install -y python3 python3-pip

# Install Python packages
pip3 install pymongo==3.12.3 redis==4.1.1 pandas==1.3.5

# Note: Versions are based on the knowledge cut-off. If new versions are available, please update accordingly.
