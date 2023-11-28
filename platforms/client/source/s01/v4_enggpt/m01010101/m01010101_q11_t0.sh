#!/bin/bash
# This script installs the required dependencies for the Python script.

# Install Python 3 and pip if not installed
sudo apt-get update
sudo apt-get install -y python3 python3-pip

# Install the required Python packages
pip3 install pymysql pymongo
