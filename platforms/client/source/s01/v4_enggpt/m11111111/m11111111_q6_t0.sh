#!/bin/bash
# install_dependencies.sh

# Update package list
apt-get update

# Install Python3 and pip if not installed
apt-get install -y python3 python3-pip

# Upgrade pip to the latest version
pip3 install --upgrade pip

# Install pymongo
pip3 install pymongo
