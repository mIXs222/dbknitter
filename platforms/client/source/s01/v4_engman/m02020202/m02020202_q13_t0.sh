#!/bin/bash
# install_dependencies.sh

# Update package lists
sudo apt-get update

# Install Python 3 and PIP if they are not installed
sudo apt-get install -y python3 python3-pip

# Install Python libraries required for the script
pip install pymysql pandas direct-redis
