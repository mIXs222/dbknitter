#!/bin/bash

# install_dependencies.sh

# Update the package lists
sudo apt-get update

# Install Python3 and Pip if not already present
sudo apt-get install -y python3 python3-pip

# Install Python requirements
pip3 install pandas direct-redis
