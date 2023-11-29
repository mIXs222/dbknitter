#!/bin/bash

# Install pip if not installed
which pip3 &>/dev/null || {
    echo "Installing pip..."
    apt-get update && apt-get install -y python3-pip
}

# Install required Python packages
pip3 install pymysql pymongo
