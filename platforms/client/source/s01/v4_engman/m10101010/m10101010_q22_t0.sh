#!/bin/bash

# Install pip if it's not available
if ! command -v pip &> /dev/null; then
    echo "pip could not be found, installing..."
    apt-get update && apt-get install -y python3-pip
fi

# Install the required Python libraries
pip3 install pymysql pymongo
