#!/bin/bash
# install_dependencies.sh

# Create a virtual environment
python3 -m venv venv

# Activate the virtual environment
source venv/bin/activate

# Install pandas library
pip install pandas

# Install the direct_redis library. Note that if it's not a standard library available through pip,
# this will fail and you'd need to obtain and install it through other means.
pip install direct_redis

# Deactivate the virtual environment
deactivate
