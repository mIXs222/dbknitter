#!/bin/bash

# Update system
sudo apt-get update

# Install pip if it's not already installed
sudo apt-get install -y python3-pip

# Install Python libraries
pip3 install pymysql pymongo pandas direct-redis
