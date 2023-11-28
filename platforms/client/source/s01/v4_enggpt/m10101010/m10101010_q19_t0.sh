#!/bin/bash
# install_dependencies.sh

# Update package list and upgrade existing packages
apt-get update && apt-get upgrade -y

# Install pip if it's not already available
apt-get install -y python3-pip

# Install the Python libraries required for the script
pip3 install pymysql pymongo
