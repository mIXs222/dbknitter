#!/bin/bash

# Update the package list and Upgrade the system
sudo apt-get update && sudo apt-get -y upgrade

# Install Python3 and pip if they are not installed
sudo apt-get install -y python3
sudo apt-get install -y python3-pip

# Install pymongo using pip
pip3 install pymongo
