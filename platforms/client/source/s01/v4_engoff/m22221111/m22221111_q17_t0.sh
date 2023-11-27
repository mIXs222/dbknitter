#!/bin/bash

# Create a virtual environment
python3 -m venv venv

# Activate the virtual environment
source venv/bin/activate

# Upgrade pip
pip install --upgrade pip

# Install dependencies
pip install pymongo pandas redis direct_redis

# Add execute permissions to the python script
chmod +x query_code.py

# Deactivate the virtual environment
deactivate
