#!/bin/bash

# Install Python pip if not installed
if ! command -v pip &> /dev/null
then
    echo "pip could not be found, installing pip..."
    sudo apt update
    sudo apt install -y python3-pip
fi

# Install Python dependencies
pip install pymysql pandas redis direct-redis

# Output message
echo "All dependencies are installed"
