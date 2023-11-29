#!/bin/bash
# Make sure to execute this script with root privileges

# Update package lists
apt update

# Install Python if not already installed
apt install -y python3

# Install pip if not already installed
apt install -y python3-pip

# Install required Python packages
pip3 install pymysql pandas direct_redis
