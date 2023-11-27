#!/bin/bash

# Ensure that Python3 and PIP are installed
sudo apt-get update && sudo apt-get install -y python3 python3-pip

# Install the pymongo Python package
pip3 install pymongo
