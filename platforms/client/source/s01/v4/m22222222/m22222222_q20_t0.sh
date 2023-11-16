#!/bin/bash

# Update package list and upgrade packages
sudo apt-get update
sudo apt-get -y upgrade

# Install pip for Python 3
sudo apt-get install -y python3-pip

# Install pandas and the custom direct_redis package
# Assumption: The direct_redis package is available within the context where this script is run
pip3 install pandas direct_redis

# Mark this script as executable with:
# chmod +x install_dependencies.sh

# Then run the script with:
# ./install_dependencies.sh
