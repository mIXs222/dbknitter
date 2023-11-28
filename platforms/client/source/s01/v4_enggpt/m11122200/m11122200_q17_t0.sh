#!/bin/bash

# Bash script to install dependencies for Python code execution

# Update package list and install Python 3 and Pip package manager
sudo apt-get update
sudo apt-get install -y python3 python3-pip

# Install the required Python libraries
pip3 install pymysql pymongo pandas
