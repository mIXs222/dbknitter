#!/bin/bash
# install_dependencies.sh

# Update package list and upgrade packages
sudo apt-get update
sudo apt-get -y upgrade

# Install Python and Pip
sudo apt-get install -y python3 python3-pip

# Install required Python libraries
pip3 install pymysql pymongo pandas direct-redis

# Optionally, if you need to install MongoDB and Redis servers
# sudo apt-get install -y mongodb redis-server

# Notes: 
# 1. This script assumes that you are using a Debian/Ubuntu-based system.
# 2. The `sudo` prefix might not be needed if you are running as root.
# 3. The installation of MongoDB and Redis servers is optional as the script does not require running the servers, only the client libraries.
