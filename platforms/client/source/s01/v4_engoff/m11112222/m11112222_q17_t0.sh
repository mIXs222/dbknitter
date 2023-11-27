#!/bin/bash

# Update package list
sudo apt-get update

# Install Python and Pip if not installed (Debian/Ubuntu)
sudo apt-get install -y python3 python3-pip

# Install MongoDB dependencies
sudo apt-get install -y libssl-dev

# Install pymongo and direct_redis via pip
pip3 install pymongo direct_redis pandas
