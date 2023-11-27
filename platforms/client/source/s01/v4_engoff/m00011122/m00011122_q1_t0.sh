#!/bin/bash

# Create a virtual environment for the project
python3 -m venv venv

# Activate the virtual environment
source venv/bin/activate

# Install pandas
pip install pandas

# Install direct_redis (it should be provided since it's a custom package)
# If it's a package that can be installed from PyPI or another package index, use pip install.
# Otherwise, if it's a local module, you'd need to set it up manually in your environment.
# For the sake of this example, I'm assuming it's something that can be installed via pip.
pip install direct_redis

# Deactivate the virtual environment
deactivate
