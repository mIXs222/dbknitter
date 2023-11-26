#!/bin/bash

# Assume you have Python3 already installed
# Update your package list
sudo apt update

# Install pip for Python3 if it's not already installed
sudo apt install -y python3-pip

# Install pymongo, make sure pip is for Python3
pip3 install pymongo
