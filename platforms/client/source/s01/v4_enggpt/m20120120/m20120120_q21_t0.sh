#!/bin/bash

# Update package manager and packages
sudo apt-get update
sudo apt-get -y upgrade

# Install Python 3 and pip if not available
sudo apt-get install -y python3
sudo apt-get install -y python3-pip

# Install required Python packages
pip3 install pymysql pandas direct-redis
