#!/bin/bash
# Bash script to install required Python libraries.

# Update the package lists
sudo apt-get update

# Install Python and pip if they are not already installed
sudo apt-get install -y python3 python3-pip

# Install the required Python dependencies
pip3 install pymysql pandas direct-redis
