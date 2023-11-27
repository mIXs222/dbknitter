#!/bin/bash
# This script installs all the dependencies required to run the Python code.

# Update package list and upgrade existing packages
sudo apt update && sudo apt upgrade -y

# Install Python3 and pip3 if not already installed
sudo apt install -y python3 python3-pip

# Install pymongo for Python
pip3 install pymongo
