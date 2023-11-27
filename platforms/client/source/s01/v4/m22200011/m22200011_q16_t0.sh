#!/bin/bash

# Update package list and install Python, pip, and other necessary system packages
sudo apt-get update
sudo apt-get install -y python3 python3-pip

# Install necessary Python packages
pip3 install pymysql pandas redis direct-redis

# Prepare Python environment variables (if needed for the script to work)
# export ...

# Run the Python script to execute the query and save the output
python3 query_code.py
