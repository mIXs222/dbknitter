#!/bin/bash

# Make sure Python3 and PIP are installed
sudo apt-get update
sudo apt-get install -y python3 python3-pip

# Install pymongo package for MongoDB connections
pip3 install pymongo
