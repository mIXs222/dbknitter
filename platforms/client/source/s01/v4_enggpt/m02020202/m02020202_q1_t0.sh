#!/bin/bash

# Update repositories and upgrade the system to make sure all packages are up to date
sudo apt-get update && sudo apt-get upgrade -y

# Install pip for Python3, which is necessary to install other Python packages
sudo apt-get install -y python3-pip

# Install Python packages required for the script
pip3 install pandas redis direct-redis
