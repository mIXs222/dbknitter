#!/bin/bash

# Update package lists
sudo apt-get update

# Install Python 3 and pip if they are not installed
sudo apt-get install -y python3 python3-pip

# Install pymongo, pandas, and redis
pip3 install pymongo pandas

# Assuming direct_redis is a special package that we have access to.
# If it is a package available in PyPi, then we would install it using pip with the command below, but this command could be different depending on how direct_redis should be installed.
pip3 install direct_redis
