#!/bin/bash

# install_dependencies.sh
# Run this script to install all the necessary dependencies for the Python code

# Update package lists
sudo apt update

# Install Python3 and pip if they are not installed
sudo apt install python3 -y
sudo apt install python3-pip -y

# Install the Python packages
pip3 install pymongo pandas redis
