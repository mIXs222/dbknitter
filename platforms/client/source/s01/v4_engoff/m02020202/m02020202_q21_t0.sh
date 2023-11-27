#!/bin/bash

# install_dependencies.sh

# Update repositories and install pip if not already installed
sudo apt-get update
sudo apt-get install -y python3-pip

# Install required Python packages
pip3 install pandas pymysql direct-redis
