#!/bin/bash

# Update repositories and install Python (if not already installed)
sudo apt update
sudo apt install -y python3

# Install Pip for Python 3
sudo apt install -y python3-pip

# Install Python libraries
pip3 install pymysql pandas direct-redis
