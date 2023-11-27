#!/bin/bash
# install_dependencies.sh

# Update the package index
sudo apt-get update

# Install Python and pip
sudo apt-get install -y python3 python3-pip

# Install pandas
pip3 install pandas

# Install direct_redis, no such library exists but we assume it is provided by the user
pip3 install direct_redis
