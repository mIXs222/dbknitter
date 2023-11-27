#!/bin/bash
# Bash script to install dependencies for the Python code

# Update package lists
sudo apt-get update

# Install Python3 and pip if not already installed
sudo apt-get install -y python3 python3-pip

# Install pymysql library for Python3
pip3 install pymysql
