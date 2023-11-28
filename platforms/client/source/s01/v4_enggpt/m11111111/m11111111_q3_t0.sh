#!/bin/bash

# install_dependencies.sh
# Install Python 3 and Pymongo
sudo apt-get update
sudo apt-get install -y python3
sudo apt-get install -y python3-pip
pip3 install pymongo
