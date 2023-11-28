#!/bin/bash

# Update package lists
sudo apt-get update

# Install Python3 if not already installed
sudo apt-get install -y python3

# Install pip for Python3 if not already installed
sudo apt-get install -y python3-pip

# Create a Python virtual environment (optional but recommended)
python3 -m venv venv
source venv/bin/activate

# Install pandas
pip install pandas

# Install direct_redis module (assuming it is available through pip)
pip install direct_redis
