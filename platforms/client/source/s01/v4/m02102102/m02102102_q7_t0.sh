#!/bin/bash

# Install Python 3 and pip if they are not installed
if ! command -v python3 &> /dev/null; then
    apt-get update
    apt-get install -y python3
fi

if ! command -v pip &> /dev/null; then
    apt-get install -y python3-pip
fi

# Install the required Python libraries
pip install pymysql
pip install pymongo
pip install pandas
pip install direct-redis
