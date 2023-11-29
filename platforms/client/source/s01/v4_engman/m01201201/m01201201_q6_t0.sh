#!/bin/bash

# Update the package lists
sudo apt-get update

# Install Python3 and pip3 if they're not installed already
sudo apt-get install -y python3 python3-pip

# Install pymongo library using pip3
pip3 install pymongo
