#!/bin/bash

# Update package lists
apt-get update

# Install Python 3 and Pip if they are not installed
apt-get install -y python3 python3-pip

# Upgrade pip to the latest version
pip3 install --upgrade pip

# Install the necessary Python packages
pip3 install pandas

# Install direct_redis from PyPi or another source if necessary
# The installation command depends on where direct_redis is hosted
# If it is on PyPi, you can use the following command:
pip3 install direct_redis

# If direct_redis is not on PyPi, and you have a specific link to the package, use:
#pip3 install <link_to_direct_redis_package>
