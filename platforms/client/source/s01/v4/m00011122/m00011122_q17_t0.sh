#!/bin/bash

# Update package manager (assumes Debian-based system)
sudo apt-get update

# Install pip for Python package management
sudo apt-get install -y python3-pip

# Install Python MySQL client
pip3 install pymysql

# Install Python Redis client
pip3 install direct-redis pandas
