#!/bin/bash
# This script installs all the necessary dependencies for running the Python code.
# Ensure that Python 3 and pip are already installed on your system.

# Update package list and upgrade existing packages
sudo apt update && sudo apt upgrade -y

# Install the required Python packages
pip install pymysql pymongo pandas direct_redis
