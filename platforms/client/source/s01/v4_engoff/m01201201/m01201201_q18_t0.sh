#!/bin/bash

# install_dependencies.sh

# Update repositories and upgrade the system
sudo apt-get update
sudo apt-get upgrade -y

# Install Python and Pip if not already installed
sudo apt-get install python3 -y
sudo apt-get install python3-pip -y

# Install required Python packages
pip3 install pandas pymysql pymongo direct_redis
