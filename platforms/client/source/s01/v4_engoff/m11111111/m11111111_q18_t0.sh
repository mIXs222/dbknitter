#!/bin/bash
# setup.sh

# Update and Install Python3 and Pip if not already installed
sudo apt-get update
sudo apt-get install -y python3
sudo apt-get install -y python3-pip

# Install pymongo
pip3 install pymongo
