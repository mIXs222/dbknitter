#!/bin/bash
# install_dependencies.sh

# Update package list and install pip for Python 3
sudo apt-get update
sudo apt-get install -y python3-pip

# Install necessary Python packages
pip3 install pandas
pip3 install direct_redis
