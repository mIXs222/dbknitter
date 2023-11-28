#!/bin/bash

# Update the package list
sudo apt-get update

# Install pip and Python development headers (they might be used for some Python package compilations)
sudo apt-get install -y python3-pip python3-dev

# Install necessary Python packages
pip3 install pymongo pandas direct-redis

# Note: The above command assumes direct_redis is a Python package that can be installed via pip.
# However, as of the knowledge cutoff date, it's not registered in PyPI.
# If that's the case, you may have to install it from the source or an alternative method provided by the package maintainer.
