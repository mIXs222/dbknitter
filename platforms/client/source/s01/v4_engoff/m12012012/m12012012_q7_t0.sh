#!/bin/bash

# Update package list and install pip if it's not installed
sudo apt-get update
sudo apt-get install -y python3-pip

# Install Python packages
pip3 install pymysql pymongo pandas direct-redis
