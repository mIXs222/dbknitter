# install_dependencies.sh

#!/bin/bash

# Update packages and upgrade the system
sudo apt-get update && sudo apt-get upgrade -y

# Install pip for Python package management
sudo apt-get install -y python3-pip

# Install MySQL client
sudo apt-get install -y default-mysql-client

# Install Redis tools
sudo apt-get install -y redis-tools

# Make sure to install pandas, pymysql, pymongo and direct_redis Python packages
pip3 install pandas pymysql pymongo direct_redis

# Note: The user should be aware that they need MongoDB and Redis servers running and accessible
