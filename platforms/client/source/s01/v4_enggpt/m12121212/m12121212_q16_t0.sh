#!/bin/bash
# install_dependencies.sh

# Update package list
apt-get update

# Install Python pip if not already installed
apt-get install -y python3-pip

# Install required Python packages
pip3 install pymongo
pip3 install direct_redis
pip3 install pandas
