#!/bin/bash

# Ensure pip, python, and virtualenv are installed
apt-get update && apt-get install -y python3-pip python3-dev
pip3 install --upgrade pip

# Create a virtual environment
python3 -m venv query_env
source query_env/bin/activate

# Install Python dependencies
pip install pymongo pandas direct-redis

# Deactivate the virtual environment
deactivate
