#!/bin/bash

# install_dependencies.sh

# Update package list and install Python pip and MongoDB
sudo apt update
sudo apt install -y python3-pip mongodb-clients

# Install the required Python packages
pip3 install pymongo
