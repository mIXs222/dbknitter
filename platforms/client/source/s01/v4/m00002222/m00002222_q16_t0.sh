#!/bin/bash
# install_dependencies.sh

# Update package list and install Python and pip
sudo apt-get update
sudo apt-get install -y python3 python3-pip

# Upgrade pip to the latest version
python3 -m pip install --upgrade pip

# Install Python dependencies required for the script
pip3 install pandas pymysql redis direct-redis
