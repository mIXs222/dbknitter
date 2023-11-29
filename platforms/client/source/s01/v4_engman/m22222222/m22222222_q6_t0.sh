#!/bin/bash
# install_dependencies.sh

# Update the package list
apt-get update

# Install Python and Pip if they're not already installed
apt-get install -y python3 python3-pip

# Install pandas
pip3 install pandas

# Install the necessary redis library
pip3 install git+https://github.com/haveyoudebuggedit/direct-redis.git
