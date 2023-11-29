#!/bin/bash
# install_dependencies.sh

# Update the package list
apt-get update

# Install Python 3 if not already installed
apt-get install -y python3

# Install pip for Python 3 if not already installed
apt-get install -y python3-pip

# Install pandas
pip3 install pandas

# Assuming direct_redis and its dependencies are already provided or available.
# Replace direct-redis-package.whl with the actual wheel package of direct_redis or the command to install it from the repository.
# pip3 install direct-redis-package.whl
