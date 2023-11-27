#!/bin/bash
# This script is for installing the required dependencies for Python code

# Update the package list
apt-get update

# Install pip if it's not available
apt-get install -y python3-pip

# Install Python packages
pip3 install pymysql pandas direct-redis
