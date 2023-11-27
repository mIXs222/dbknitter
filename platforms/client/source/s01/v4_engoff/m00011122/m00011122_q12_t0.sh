#!/bin/bash

# Update repository and upgrade packages
sudo apt-get update -y
sudo apt-get upgrade -y

# Install pip if not installed
sudo apt-get install -y python3-pip

# Create a virtual environment for the project
python3 -m venv venv

# Activate the virtual environment
source venv/bin/activate

# Install the pandas library
pip install pandas

# Install the direct_redis library
pip install direct-redis

# Deactivate the virtual environment
deactivate
