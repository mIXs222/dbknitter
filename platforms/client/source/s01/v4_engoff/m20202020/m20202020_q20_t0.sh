#!/bin/bash

# Update package lists
sudo apt-get update

# Install Python
sudo apt-get install -y python3 python3-pip

# Install Python packages
pip3 install pymysql pandas sqlalchemy direct-redis
