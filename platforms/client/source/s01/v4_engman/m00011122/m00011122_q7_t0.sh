#!/bin/bash
# This script installs the necessary Python libraries to run the Python code.
# Note: This script assumes Python3 and pip are already installed on the system.

# Update and upgrade the package list
apt-get update && apt-get upgrade -y

# Install Python packages
pip install pandas pymysql pymongo direct-redis
