#!/bin/bash

# Update system packages
sudo apt-get update

# Install pip for Python 3
sudo apt-get install -y python3-pip

# Install pymongo to work with MongoDB from Python
pip3 install pymongo
