#!/bin/bash
# install.sh

# Update package list and upgrade existing packages
apt-get update
apt-get upgrade -y

# Install Python and pip if not already installed
apt-get install python3 python3-pip -y

# Install the required Python package (pymongo)
pip3 install pymongo
