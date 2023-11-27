#!/bin/bash

# Install dependencies script (install_dependencies.sh)

# Make sure system package list is updated
sudo apt-get update

# Install Python3 and pip if not already installed
sudo apt-get install -y python3 python3-pip

# Upgrade pip to the latest version
pip3 install --upgrade pip

# Install Python libraries required for the script to run
pip3 install pymysql pymongo
