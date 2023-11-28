#!/bin/bash
# Bash script to install all dependencies to run the provided Python code.

# Update package lists
sudo apt-get update

# Install Python and pip, if not already installed
sudo apt-get install -y python3 python3-pip

# Install the required Python libraries
pip3 install pymysql pandas redis direct-redis
