#!/bin/bash
# install_dependencies.sh

# Update package list
sudo apt-get update

# Install pip for Python
sudo apt-get install -y python3-pip

# Install pandas
pip3 install pandas

# Install custom direct_redis package (assuming it is available via pip or add the corresponding command to install it from the correct source)
pip3 install direct_redis
