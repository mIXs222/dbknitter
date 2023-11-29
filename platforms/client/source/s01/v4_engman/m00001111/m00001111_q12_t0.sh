#!/bin/bash

# Update package list
sudo apt-get update

# Install Python MongoDB driver (pymongo)
sudo apt-get install -y python3-pip
pip3 install pymongo
