#!/bin/bash

# Create a virtual environment
python3 -m venv venv

# Activate the virtual environment
source venv/bin/activate

# Upgrade pip
pip install --upgrade pip

# Install pandas
pip install pandas

# Install direct_redis (Assuming it's available on PyPI or replace it with the proper repository)
pip install direct_redis
