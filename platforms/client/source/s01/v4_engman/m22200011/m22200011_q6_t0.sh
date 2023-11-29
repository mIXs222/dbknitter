#!/bin/bash

# setup.sh
# Update package list, upgrade packages, and install python3 and pip
sudo apt-get update
sudo apt-get -y upgrade
sudo apt-get install -y python3
sudo apt-get install -y python3-pip

# Install the pymongo package
pip3 install pymongo
