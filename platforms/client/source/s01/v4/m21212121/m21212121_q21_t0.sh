#!/bin/bash

# install_dependencies.sh

# Set up a Python virtual environment
python3 -m venv venv
source venv/bin/activate

# Install Python dependencies
pip install pymongo pandas redis msgpack direct_redis

# Deactivate virtual environment
deactivate
