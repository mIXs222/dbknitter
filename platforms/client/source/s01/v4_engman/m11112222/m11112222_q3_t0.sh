#!/bin/bash
# install_dependencies.sh

# Update package lists and upgrade existing packages
apt-get update
apt-get -y upgrade

# Install Python and pip, if they aren't already installed
apt-get install -y python3
apt-get install -y python3-pip

# Install required Python packages
pip3 install pandas redis direct_redis
