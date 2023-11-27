#!/bin/bash

# Create a virtual environment and activate it
python3 -m venv venv
source venv/bin/activate

# Install required dependencies
pip install pymongo pandas direct-redis

# Deactivate the virtual environment
deactivate
