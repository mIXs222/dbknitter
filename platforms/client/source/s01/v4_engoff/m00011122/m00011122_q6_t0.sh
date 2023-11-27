#!/bin/bash
# install_dependencies.sh

# Update package list
sudo apt-get update

# Install pip and Python setuptools
sudo apt-get install -y python3-pip python3-setuptools

# Install pandas
pip3 install pandas

# Install direct_redis (assuming that the package is available)
# Replacement for direct_redis may be needed if it is not a standard PyPI package
pip3 install direct_redis

# Or install redis-py if direct_redis is not a PyPI package and you have an alternative solution or package
pip3 install redis
