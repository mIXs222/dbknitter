#!/bin/bash

# Update repositories and install Python3 and pip if not installed
sudo apt-get update

# Check if Python is installed
if ! python3 --version &>/dev/null; then
    sudo apt-get install -y python3
fi

# Check if pip is installed
if ! pip3 --version &>/dev/null; then
    sudo apt-get install -y python3-pip
fi

# Install PyMySQL library
pip3 install pymysql
