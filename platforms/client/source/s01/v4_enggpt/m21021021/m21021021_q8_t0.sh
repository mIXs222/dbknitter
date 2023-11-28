#!/bin/bash

# Update package lists
apt-get update

# Install Python and pip (Assuming Debian-based system)
apt-get install -y python3 python3-pip

# Install required Python packages
pip3 install pymongo pymysql pandas

# Since direct_redis is not a standard package and has no information about its installation, the following is commented out.
# Please provide specific installation instructions if it's a custom or private package.
# pip3 install direct_redis
