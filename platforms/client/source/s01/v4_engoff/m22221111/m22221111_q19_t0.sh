#!/bin/bash

# install_dependencies.sh

# Install Python3 and Pip if they aren't already installed
sudo apt-get update
sudo apt-get install -y python3 python3-pip

# Install needed python libraries
pip3 install pymongo pandas redis direct_redis

# Run the Python script
python3 discounted_revenue_query.py
