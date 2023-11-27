#!/bin/bash
# Filename: install_dependencies.sh

# Update package list and install pip if not already installed
apt-get update
apt-get install -y python3-pip

# Install the necessary Python libraries
pip3 install pymysql pandas redis direct-redis
