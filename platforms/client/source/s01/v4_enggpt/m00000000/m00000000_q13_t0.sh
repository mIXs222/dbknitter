#!/bin/bash
# This script is to install Python and required modules for the project

# Update package index
sudo apt update

# Install Python3 and Python3-pip if they are not installed
sudo apt install -y python3 python3-pip

# Install the pymysql package using pip
pip3 install pymysql
