#!/bin/bash

# Update package lists
apt-get update

# Install Python and pip if they are not installed
apt-get install -y python3 python3-pip

# Create a virtual environment (optional but recommended)
python3 -m venv venv
source venv/bin/activate

# Install required Python packages
pip install pymongo pandas

# Since direct_redis might not be available in PyPI, install it from source or custom repository if available.
# Placeholder for direct_redis installation command.
# Insert here the commands to install the direct_redis library.
