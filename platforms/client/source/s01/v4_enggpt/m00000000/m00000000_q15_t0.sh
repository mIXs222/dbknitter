#!/bin/bash

# Update package manager
sudo apt-get update

# Install Python 3 and Pip (if they're not already installed)
sudo apt-get install -y python3 python3-pip

# Install PyMySQL
pip3 install pymysql
