#!/bin/bash
# setup.sh

# Create a new virtual environment
python3 -m venv venv

# Activate the virtual environment
source venv/bin/activate

# Upgrade pip
pip install --upgrade pip

# Install dependencies
pip install pandas redis

# Note: Since `direct_redis.DirectRedis` is not a real Python package
# available on PyPI, we're assuming it's a custom implementation that has to be provided.
# If `direct_redis` is a package available via pip, it would be installed with `pip install direct_redis`.
# If it's a local module, make sure to include it in the same directory as the script.
