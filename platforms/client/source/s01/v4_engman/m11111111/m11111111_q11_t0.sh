#!/bin/bash
# install_dependencies.sh

# Update package list
apt-get update

# Install python3 and pip if they are not installed
apt-get install -y python3 python3-pip

# Install pymongo module
pip3 install pymongo
