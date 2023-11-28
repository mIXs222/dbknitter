#!/bin/bash

# Bash script to install dependencies for running the Python code

# Update package list
apt-get update

# Install pip for Python if not already installed
apt-get install -y python3-pip

# Install the required Python libraries
pip3 install pymysql pymongo
