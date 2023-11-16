#!/bin/bash

# Update package list and upgrade system
sudo apt-get update
sudo apt-get -y upgrade

# Create a virtual environment for the project
python3 -m venv env

# Activate the virtual environment
source env/bin/activate

# Install pandas
pip install pandas

# Install direct_redis (in this fictional case, if it were a real library)
pip install direct_redis

# Activate permissions for execution
chmod +x query_execution.py
