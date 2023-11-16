#!/bin/bash

# Update package list and install pip for Python3
sudo apt-get update
sudo apt-get install -y python3-pip

# Create a virtual environment for the Python project (optional but recommended)
python3 -m pip install --user virtualenv
python3 -m venv env
source env/bin/activate

# Install necessary Python packages
pip install pandas
pip install direct_redis
