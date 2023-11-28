#!/bin/bash
# This script is used to install dependencies for the Python code provided above

# Update package list
sudo apt-get update

# Install Python3, pip and MySQL client
sudo apt-get install python3 python3-pip mysql-client -y

# Install the pymysql package using pip
pip3 install pymysql
