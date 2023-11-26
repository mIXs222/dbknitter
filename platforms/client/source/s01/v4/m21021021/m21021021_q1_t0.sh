#!/bin/bash

# Update the package list and upgrade existing packages
sudo apt-get update -y && sudo apt-get upgrade -y

# Install Python 3 and pip (if not already installed)
sudo apt-get install python3 -y
sudo apt-get install python3-pip -y

# Install pymongo Python library
pip3 install pymongo
