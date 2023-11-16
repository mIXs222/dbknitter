#!/bin/bash
# install_dependencies.sh

# Update system package index
sudo apt-get update

# Install pip if it's not already installed
sudo apt-get install -y python3-pip

# Install Python dependencies
pip3 install pymysql pandas sqlalchemy direct-redis
