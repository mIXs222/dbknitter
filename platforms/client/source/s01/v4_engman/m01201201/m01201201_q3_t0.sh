#!/bin/bash

# Create a virtual environment
python3 -m venv venv

# Activate the virtual environment
source venv/bin/activate

# Upgrade pip
pip install --upgrade pip

# Install the required packages
pip install pymysql pymongo pandas direct-redis

# Run the python code
python execute_query.py

# Deactivate the virtual environment
deactivate
