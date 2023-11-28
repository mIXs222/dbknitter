#!/bin/bash

# Update package lists
sudo apt-get update

# Install Python 3 and Pip if they are not installed
sudo apt-get install -y python3 python3-pip

# Install MongoDB
sudo apt-get install -y mongodb

# Install Redis
sudo apt-get install -y redis-server

# Install Python dependencies
pip3 install pymongo direct_redis pandas
