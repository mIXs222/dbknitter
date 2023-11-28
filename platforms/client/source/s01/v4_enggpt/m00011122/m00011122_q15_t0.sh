#!/bin/bash

# Update package list and install Python pip
sudo apt update
sudo apt install -y python3-pip

# Install required Python packages
pip3 install pymongo pandas direct-redis
