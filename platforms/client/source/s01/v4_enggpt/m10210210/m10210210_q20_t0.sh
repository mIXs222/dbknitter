#!/bin/bash

# Bash script to install Python dependencies for running the Python code

# Update package list
sudo apt-get update

# Install pip and Python dev tools
sudo apt-get install python3-pip python3-dev -y

# Use pip to install the required libraries
pip3 install pandas pymysql pymongo redis direct-redis
