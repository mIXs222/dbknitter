#!/bin/bash
# install_dependencies.sh

# Update package list and upgrade existing packages
sudo apt-get update && sudo apt-get upgrade -y

# Install pip if not already installed
sudo apt-get install -y python3-pip

# Install required Python libraries
pip install pymysql pymongo pandas direct-redis
