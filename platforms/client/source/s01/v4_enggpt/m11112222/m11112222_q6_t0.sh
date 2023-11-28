#!/bin/bash

# This script is for setting up a Python environment and installing the required libraries.

# Create a virtual environment for Python
python -m venv venv

# Activate the virtual environment
source venv/bin/activate

# Upgrade pip to the latest version
pip install --upgrade pip

# Install pandas
pip install pandas

# Install direct_redis (assuming this is a Python package you have access to which acts similar to redis-py but with added functionality)
pip install direct_redis
