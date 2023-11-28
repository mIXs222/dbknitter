#!/bin/bash

# Assuming you're on a Debian-based system (like Ubuntu)
# Update the package index
sudo apt-get update

# Install Python and pip (if not already installed)
sudo apt-get install -y python3 python3-pip

# Install the required Python libraries
pip3 install pymysql pandas
