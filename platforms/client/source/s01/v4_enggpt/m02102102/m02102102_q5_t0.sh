#!/bin/bash

# Create a virtual environment
python3 -m venv venv

# Activate the virtual environment
source venv/bin/activate

# Update pip
pip install --upgrade pip

# Install required dependencies
pip install pymysql pymongo pandas direct_redis

# Deactivate the virtual environment
deactivate
