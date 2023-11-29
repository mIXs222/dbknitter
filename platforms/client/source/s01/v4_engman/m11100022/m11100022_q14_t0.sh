#!/bin/bash

# Bash script to install all required dependencies for the Python code to run

# Update package lists
apt-get update

# Install Python 3 and pip if they are not installed
apt-get install -y python3 python3-pip

# Install required Python packages
pip3 install pandas pymongo direct_redis
