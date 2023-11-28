#!/bin/bash
# install_dependencies.sh

# Assuming usage of a Debian-based system (e.g., Ubuntu)

# Update package lists
sudo apt-get update

# Install pip for Python 3
sudo apt-get install -y python3-pip

# Install Redis
sudo apt-get install -y redis-server

# Enable Redis to start on system boot
sudo systemctl enable redis-server.service

# Install Python libraries
pip3 install pandas
pip3 install direct-redis
