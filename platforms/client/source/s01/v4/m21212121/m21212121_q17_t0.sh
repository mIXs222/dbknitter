#!/bin/bash

# install_dependencies.sh

# Update packages and install python3, pip and redis
sudo apt-get update
sudo apt-get install -y python3 python3-pip

# Install required Python packages
pip3 install pymongo pandas direct_redis
