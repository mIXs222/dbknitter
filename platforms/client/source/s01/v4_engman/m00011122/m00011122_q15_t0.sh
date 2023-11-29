#!/bin/bash
# install_dependencies.sh

# Update system
apt-get update

# Install pip and Python headers (if not already installed)
apt-get install -y python3-pip python3-dev 

# Install required Python packages
pip3 install pymongo pandas "direct-redis"

# Note: The user must ensure that pip3 corresponds to the version of Python the script is written for.
