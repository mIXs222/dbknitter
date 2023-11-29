#!/bin/bash

# install_dependencies.sh

# Update package lists
sudo apt-get update

# Install Python3 and pip if not installed
sudo apt-get install -y python3 python3-pip

# Install required Python libraries
pip3 install pymysql pandas direct-redis
