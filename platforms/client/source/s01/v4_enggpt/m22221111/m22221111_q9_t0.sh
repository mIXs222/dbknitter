#!/bin/bash

# Update package lists
apt-get update

# Install Python and pip
apt-get install -y python3 python3-pip

# Install Python packages
pip3 install pymongo pandas direct_redis

# Note: The actual installation of MongoDB and Redis servers is not covered in this script.
# It will be assumed that these services are already installed and running.
