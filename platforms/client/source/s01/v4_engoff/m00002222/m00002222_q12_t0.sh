#!/bin/bash
# install.sh

# Update the package index
sudo apt-get update

# Install pip if it is not already installed
sudo apt-get install -y python3-pip

# Install Python dependencies
pip3 install pandas redis
