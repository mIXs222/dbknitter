#!/bin/bash
# This bash script is for installing dependencies required by the Python code

# Update package lists
apt-get update

# Install pip, a package manager for Python
apt-get install -y python3-pip

# Install Python packages required by the Python code
pip3 install pandas pymysql sqlalchemy direct-redis
