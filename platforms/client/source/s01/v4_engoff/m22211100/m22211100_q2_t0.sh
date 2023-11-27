#!/bin/bash

# install_dependencies.sh

# Update the list of packages and install python3 and pip if they are not installed
sudo apt-get update
sudo apt-get install -y python3 python3-pip

# Install required Python packages
pip3 install pymongo pandas direct-redis
