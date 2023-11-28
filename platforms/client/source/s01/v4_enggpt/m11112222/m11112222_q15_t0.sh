#!/bin/bash

# Update package lists
sudo apt update

# Install Python and pip if they are not installed
sudo apt install -y python3 python3-pip

# Install MongoDB
sudo apt install -y mongodb

# Install Redis
sudo apt install -y redis-server

# Enable Redis to start on boot
sudo systemctl enable redis-server

# Install Python packages
pip3 install pymongo pandas direct-redis
