#!/bin/bash
# Bash script (install_dependencies.sh)

# Assuming Python3 and Pip are already installed

# Update package index
apt-get update

# Install Python Redis client - direct_redis
pip install direct_redis

# Install pandas for data manipulation
pip install pandas
