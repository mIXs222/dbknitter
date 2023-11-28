# File: install_dependencies.sh
#!/bin/bash

# Update package list and upgrade packages
sudo apt-get update
sudo apt-get -y upgrade

# Install Python dependencies
sudo apt-get install -y python3-pip
pip3 install pandas pymysql sqlalchemy direct-redis

# Make the python script executable
chmod +x query.py
