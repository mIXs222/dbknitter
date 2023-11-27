#!/bin/bash

# Create a virtual environment
python3 -m venv query_env

# Activate the virtual environment
source query_env/bin/activate

# Install pandas
pip install pandas

# There isn't an actual package named direct_redis available in public PyPI at the time of my knowledge cut off.
# Assuming direct_redis is a custom module for your platform,
# you should place this module within your project or adjust the path accordingly
# For the purpose of this example, we will assume it is in the same directory.
# If 'direct_redis' was a public package, it would be installed via pip as well
# pip install direct_redis

# Deactivate the virtual environment after installation
deactivate
