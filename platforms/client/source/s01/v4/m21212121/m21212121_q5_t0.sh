#!/bin/bash

# setup.sh

# Create a Python virtual environment and activate it
python3 -m venv venv
source venv/bin/activate

# Install dependencies
pip install pymongo direct_redis pandas

# Deactivate virtual environment
deactivate
