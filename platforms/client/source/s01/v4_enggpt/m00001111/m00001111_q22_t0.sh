#!/bin/bash

# Update the package list
sudo apt-get update

# Install Python3 and pip
sudo apt-get install -y python3 python3-pip

# Install pymongo
pip3 install pymongo
