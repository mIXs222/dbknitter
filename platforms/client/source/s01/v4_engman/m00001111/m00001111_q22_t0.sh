#!/bin/bash

# Make sure the script exits on first error
set -e

# Update your system's package list
sudo apt-get update

# Install Python3 and pip if they are not installed
sudo apt-get install -y python3 python3-pip

# Install pymongo using pip
pip3 install pymongo
