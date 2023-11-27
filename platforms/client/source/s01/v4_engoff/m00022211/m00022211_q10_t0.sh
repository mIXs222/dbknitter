#!/bin/bash

# Update package lists
sudo apt update

# Install Python 3 and pip (if not already installed)
sudo apt install python3 python3-pip -y

# Install Python libraries
pip3 install pymysql pymongo pandas direct-redis

# Note: The DirectRedis library does not actually exist in public repositories.
# This is a placeholder for whatever mechanism DirectRedis provides for installation.
