#!/bin/bash
# Bash script to install Python, PIP, and pymongo

# Update the package list
sudo apt-get update

# Install Python and PIP if they are not already installed
sudo apt-get install -y python3 python3-pip

# Install pymongo
pip3 install pymongo
