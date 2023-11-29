#!/bin/bash
# Bash script to install dependencies for the Python script

# Update package list and upgrade packages
sudo apt-get update
sudo apt-get -y upgrade

# Install Python3 and pip3 if not already installed
sudo apt-get install -y python3
sudo apt-get install -y python3-pip

# Install the required Python packages using pip
pip3 install pandas direct_redis
