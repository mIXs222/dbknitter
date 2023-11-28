#!/bin/bash

# Create a new Python virtual environment
python3 -m venv venv
source venv/bin/activate

# Update pip to its latest version
pip install --upgrade pip

# Install Python dependencies
pip install pandas
pip install direct_redis

# Add any additional dependency installations below
# ...
