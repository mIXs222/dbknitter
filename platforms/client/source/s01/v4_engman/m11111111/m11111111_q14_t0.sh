#!/bin/bash

# Update the system's package index
sudo apt-get update

# Ensure that Python3 and pip are installed
sudo apt-get install -y python3 python3-pip

# Install pymongo using pip
pip3 install pymongo
