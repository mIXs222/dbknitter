#!/bin/bash

# Create a virtual environment
python3 -m venv venv

# Activate virtual environment
source venv/bin/activate

# Install redis-py and pandas
pip install redis pandas

# Install custom DirectRedis (needs to be available from an accessible package repository or local)
pip install direct-redis-package

# Replace "direct-redis-package" with the correct package name for direct_redis if it's available in a package repository
