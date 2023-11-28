# install_dependencies.sh

#!/bin/bash

# Update and upgrade the package list
sudo apt-get update
sudo apt-get -y upgrade

# Install pip for Python package management
sudo apt-get install -y python3-pip

# Create a virtual environment
python3 -m venv query_env
source query_env/bin/activate

# Install Pandas for data manipulation
pip install pandas

# Assuming the hypothetical "direct_redis" package can be installed via pip
pip install direct_redis

# Make the query script executable
chmod +x query.py
