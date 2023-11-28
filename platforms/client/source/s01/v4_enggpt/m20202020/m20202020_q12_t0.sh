#!/bin/bash

# install_dependencies.sh

# Update repositories and install pip if it's not installed
sudo apt-get update
sudo apt-get install -y python3-pip

# Install Python libraries
pip3 install pymysql pandas direct_redis
