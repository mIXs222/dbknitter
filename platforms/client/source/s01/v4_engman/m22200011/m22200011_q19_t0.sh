#!/bin/bash

# Activate your virtual environment if you are using one
# source /path/to/your/venv/bin/activate

# Update pip and setuptools
pip install --upgrade pip setuptools

# Install Python dependencies
pip install pymongo pandas direct_redis
