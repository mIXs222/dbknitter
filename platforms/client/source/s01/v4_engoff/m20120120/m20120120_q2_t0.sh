#!/bin/bash
# install_dependencies.sh

# Update and Upgrade the System
sudo apt-get update
sudo apt-get -y upgrade

# Install Python pip and dependencies
sudo apt-get install -y python3-pip

# Install required Python libraries
pip3 install pymysql pymongo pandas direct-redis
