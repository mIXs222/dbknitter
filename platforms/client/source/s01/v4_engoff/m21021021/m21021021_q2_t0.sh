#!/bin/bash

# Update packages and install pip if not already installed
sudo apt-get update
sudo apt-get install -y python3-pip

# Install the required Python packages
pip3 install pymysql pymongo pandas direct-redis
