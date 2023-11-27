#!/bin/bash
# install_dependencies.sh

# Update package list
sudo apt update

# Install Python3 and PIP if not already installed
sudo apt install python3 python3-pip -y

# Install required Python packages
pip3 install pymysql pymongo redis direct-redis pandas
