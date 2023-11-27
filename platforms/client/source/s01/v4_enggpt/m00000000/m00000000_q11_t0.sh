#!/bin/bash
# Bash script to install necessary dependencies for running the Python code.

# Update package index
sudo apt-get update

# Install Python 3 and pip if they are not installed
sudo apt-get install -y python3 python3-pip

# Install pymysql library using pip
pip3 install pymysql
