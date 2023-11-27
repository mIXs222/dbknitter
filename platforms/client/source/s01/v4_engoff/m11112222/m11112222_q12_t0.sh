#!/bin/bash
# Bash script to install the Python dependencies required to run the query.py script

# Installing Python and Pip if they are not already installed
sudo apt-get update
sudo apt-get install -y python3 python3-pip

# Create a virtual environment (optional)
python3 -m venv query_env
source query_env/bin/activate

# Install the pandas package
pip install pandas

# Installing direct_redis, assuming it is a package that can be installed via pip (Note: fictitious package for illustrative purposes)
pip install direct_redis
