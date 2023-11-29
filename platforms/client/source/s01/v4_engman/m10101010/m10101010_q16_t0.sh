#!/bin/bash
# Bash script to install dependencies (install_deps.sh)

# Update repositories and install Python and pip
sudo apt-get update
sudo apt-get install -y python3 python3-pip

# Install Python dependencies
pip3 install pymongo pymysql
