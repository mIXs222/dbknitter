#!/bin/bash
# Bash script to install dependencies for the Python code

# Update system repositories and packages
sudo apt-get update
sudo apt-get upgrade -y

# Install Python 3 and Python package manager (pip)
sudo apt-get install python3 python3-pip -y

# Install required Python packages
pip3 install pandas redis direct_redis
