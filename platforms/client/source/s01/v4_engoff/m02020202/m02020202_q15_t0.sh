#!/bin/bash

# Create a virtual environment
python3 -m venv venv
source venv/bin/activate

# Install pandas and direct_redis
pip install pandas direct_redis
