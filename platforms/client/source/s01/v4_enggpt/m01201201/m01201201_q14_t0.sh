#!/bin/bash

# Make the script fail if any command in it fails
set -e

# Update package list
apt-get update

# Install Python and Pip if they're not already installed
apt-get install -y python3 python3-pip

# Install required Python packages
pip3 install pandas pymongo redis
