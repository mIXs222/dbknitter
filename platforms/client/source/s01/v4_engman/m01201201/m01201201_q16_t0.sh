#!/bin/bash

# Update the package list on the system
sudo apt-get update

# Install pip if it's not already available
sudo apt-get install -y python3-pip

# Install required Python packages
pip3 install pymysql pymongo pandas direct-redis
