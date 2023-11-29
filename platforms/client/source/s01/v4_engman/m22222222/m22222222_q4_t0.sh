#!/bin/bash

# Set up a Python virtual environment (optional)
python3 -m venv venv
source venv/bin/activate

# Install the required packages
pip install pandas direct_redis
