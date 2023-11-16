#!/bin/bash

# Update packages list and upgrade all packages
sudo apt-get update
sudo apt-get upgrade -y

# Install Python 3.8 and pip3
sudo apt-get install python3.8 -y
sudo apt-get install python3-pip -y

# Install Python packages via pip3
pip3 install pandas redis

# Make the Python script executable
chmod +x script.py

# Run the Python script
./script.py
