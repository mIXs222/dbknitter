#!/bin/bash

# install_dependencies.sh

# Update packages and install python3 and pip if they are not already installed
sudo apt-get update
sudo apt-get install -y python3 python3-pip

# Install the required Python packages
pip3 install pymysql pandas direct-redis
