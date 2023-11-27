#!/bin/bash

# Update package list
sudo apt-get update

# Install Python3 and pip if they are not installed
sudo apt-get install -y python3
sudo apt-get install -y python3-pip

# Install Pandas
pip install pandas

# Install direct_redis, assuming it's a custom package
# This is a placeholder for how you would normally install a package
# You will need to ensure this actually points to wherever direct_redis can be installed from
pip install direct_redis
