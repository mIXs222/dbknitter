#!/bin/bash

# Update the package list
sudo apt-get update

# Install Python3 and pip3 if not already installed
sudo apt-get install -y python3 python3-pip

# Install pymongo to connect to MongoDB
pip3 install pymongo
