#!/bin/bash

# Update package lists
sudo apt-get update

# Install python3 if not installed
sudo apt-get install -y python3

# Install pip3 if not installed
sudo apt-get install -y python3-pip

# Install pymongo library
pip3 install pymongo
