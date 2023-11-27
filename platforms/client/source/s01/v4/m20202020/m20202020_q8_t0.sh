#!/bin/bash

# File name: install_dependencies.sh

# Update package lists
sudo apt-get update

# Upgrade existing packages
sudo apt-get upgrade -y

# Install Python 3 and pip if they aren't already installed
sudo apt-get install -y python3 python3-pip

# Install Python libraries
pip3 install pandas pymysql redis msgpack-python direct_redis

# Note: The user may need to run this script with superuser privileges.
