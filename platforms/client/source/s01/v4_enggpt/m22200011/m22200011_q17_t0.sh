#!/bin/bash

# Update package list
sudo apt update

# Install Python and pip if they are not already installed
sudo apt install -y python3 python3-pip

# Install required Python packages
pip3 install pymongo pandas redis
