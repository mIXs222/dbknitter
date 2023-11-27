#!/bin/bash

# Update package repository
sudo apt update

# Install Python 3 and pip if not already installed
sudo apt install -y python3 python3-pip

# Upgrade pip
pip3 install --upgrade pip

# Install Python dependencies
pip3 install pymysql pymongo pandas direct-redis
