#!/bin/bash

# Install Python 3 and pip if they are not already installed
if ! command -v python3 &> /dev/null; then
  sudo apt-get update
  sudo apt-get install -y python3
fi

if ! command -v pip3 &> /dev/null; then
  sudo apt-get install -y python3-pip
fi

# Install the required Python libraries
pip3 install pymysql pandas

# Run the Python script assuming it's named 'run_query.py'
python3 run_query.py
