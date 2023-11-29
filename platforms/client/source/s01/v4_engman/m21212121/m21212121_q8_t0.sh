#!/bin/bash

# Update package list
sudo apt-get update

# Install MongoDB
sudo apt-get install -y mongodb

# Install Redis
sudo apt-get install -y redis-server

# Install Python3 and pip
sudo apt-get install -y python3 python3-pip

# Install Python dependencies
pip3 install pandas pymongo redis direct-redis
