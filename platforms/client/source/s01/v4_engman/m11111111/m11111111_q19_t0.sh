#!/bin/bash

# Updating the package list
sudo apt-get update

# Installing Python3 and PIP if not already installed
sudo apt-get install -y python3
sudo apt-get install -y python3-pip

# Installing MongoDB driver for Python
pip3 install pymongo
