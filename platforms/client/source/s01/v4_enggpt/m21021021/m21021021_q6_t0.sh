#!/bin/bash

# Update the package list
sudo apt-get update

# Install Python 3 and pip if they are not installed
sudo apt-get install -y python3 python3-pip

# Install the pymongo library
pip3 install pymongo
