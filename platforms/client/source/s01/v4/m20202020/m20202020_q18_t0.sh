#!/bin/bash
# install_dependencies.sh

# Update the package index
sudo apt-get update

# Install Python 3, pip and other essential build tools
sudo apt-get install -y python3 python3-pip build-essential

# Install required Python packages
pip3 install pandas pymysql direct-redis
