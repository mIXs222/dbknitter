#!/bin/bash
# install_dependencies.sh

# Update the repository information
apt-get update -y

# Install Python and pip
apt-get install python3 python3-pip -y

# Install the required Python libraries
pip3 install pymongo pandas redis

# Install direct_redis separately if not available as a regular PyPI package
# The code assumes that direct_redis is a package that can be installed.
# If it's a custom library, this would need to be adapted to the correct installation method.
pip3 install direct_redis

# Run the Python script
python3 query.py
