#!/bin/bash

# Bash script to install dependencies for the Python code

# Update package list
sudo apt update

# Install python3 and pip
sudo apt install -y python3 python3-pip

# Install required Python packages
pip3 install pymongo pandas direct_redis
