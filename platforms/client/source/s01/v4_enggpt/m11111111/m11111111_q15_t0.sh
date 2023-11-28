#!/bin/bash

# Let's ensure Python3 and pip are installed
sudo apt-get update
sudo apt-get install -y python3
sudo apt-get install -y python3-pip

# Install pymongo for interacting with MongoDB
pip3 install pymongo
