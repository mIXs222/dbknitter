#!/bin/bash
# File: install_dependencies.sh
# Install Python and necessary libraries for running the script

# Install Python
sudo apt-get update
sudo apt-get install python3

# Install pip, a Python package installer
sudo apt-get install python3-pip

# Install required Python libraries
pip3 install pymysql pymongo pandas direct-redis
