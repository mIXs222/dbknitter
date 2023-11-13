#!/bin/bash
# This script should be run as root or with sudo privilege

# Update the package lists for upgrades and new package installations
sudo apt-get update

# Upgrade all upgradable packages
sudo apt-get upgrade

# Install Python3 and pip3
sudo apt-get install -y python3 python3-pip

# Install Python MongoDB Driver
pip3 install pymongo

# Install bson for MongoDB
pip3 install bson

# End of Script
