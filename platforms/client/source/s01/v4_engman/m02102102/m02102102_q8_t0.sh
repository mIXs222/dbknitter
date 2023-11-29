#!/bin/bash
# Bash script to install all dependencies required to run the Python code above

# Update package list
apt-get update

# Install pip and Python dev tools
apt-get install -y python3-pip python3-dev

# Install MySQL client
apt-get install -y default-libmysqlclient-dev

# Install Redis tools (The actual tools needed will depend on what the `DirectRedis` module entails)
apt-get install -y redis

# Install the required Python libraries
pip3 install pymysql pymongo pandas

# Note: If `direct_redis` is a fictional module or a placeholder for demonstration purposes,
# the following line is for illustrative purposes only.
pip3 install direct_redis

# Grant execute permissions to the script
chmod +x install_dependencies.sh
