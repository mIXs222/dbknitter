#!/bin/bash

# Bash script to install Python and pymysql

# Update package lists
sudo apt-get update

# Install Python3 and Python3-pip if not already installed
sudo apt-get install -y python3 python3-pip

# Install pymysql for Python using pip
pip3 install pymysql
