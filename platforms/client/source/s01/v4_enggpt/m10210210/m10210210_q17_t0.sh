#!/bin/bash
# Update the package list
sudo apt-get update

# Install Python if it's not already installed
command -v python3 >/dev/null 2>&1 || {
    sudo apt-get install -y python3
}
# Install pip if it's not already installed
command -v pip3 >/dev/null 2>&1 || {
    sudo apt-get install -y python3-pip
}

# Install required Python packages
pip3 install pandas pymysql redis direct-redis

# Ensure the script is executable
chmod +x query_exec.py

# Execute the Python script (optional in the install script, can be run separately)
# python3 query_exec.py
