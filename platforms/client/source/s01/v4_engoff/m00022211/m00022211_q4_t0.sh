#!/bin/bash
# setup.sh

# Update package list
sudo apt-get update

# Install pip if it's not already installed
sudo apt-get install -y python3-pip

# Install pymongo package
pip3 install pymongo
