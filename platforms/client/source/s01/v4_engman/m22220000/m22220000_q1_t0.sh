#!/bin/bash
# Check if the script is run as root
if [ "$EUID" -ne 0 ]
  then echo "Please run as root or use sudo"
  exit
fi

# Update package list
apt-get update

# Install Python if it is not available
command -v python3 > /dev/null 2>&1 || {
    echo "Python is not installed. Installing Python..."
    apt-get install -y python3
}

# Install pip if it is not available
command -v pip3 > /dev/null 2>&1 || {
    echo "Pip is not installed. Installing Pip..."
    apt-get install -y python3-pip
}

# Install PyMySQL using pip
pip3 install pymysql
