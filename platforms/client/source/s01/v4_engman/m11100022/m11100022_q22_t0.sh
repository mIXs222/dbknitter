#!/bin/bash

# install_dependencies.sh

# Update the package lists
sudo apt-get update

# Install Python and Pip if they are not installed
sudo apt-get install -y python3 python3-pip

# Upgrade pip
pip3 install --upgrade pip

# Install the required Python libraries
pip3 install pymysql pandas direct-redis

# Direct Redis may need a specific install, adjust the command according to the actual package name provided by the developer
