# install.sh

#!/bin/bash
# Bash script to install necessary dependencies for the Python script

# Update package lists
sudo apt-get update

# Install pip if not installed
sudo apt-get install -y python3-pip

# Install MongoDB driver
pip3 install pymongo

# Install Redis driver
pip3 install git+https://github.com/RedisLabsModules/redis-py.git

# Install pandas for data manipulation
pip3 install pandas

# Install other dependencies that may be missing
sudo apt-get install -y python3-msgpack
pip3 install msgpack

# Make the Python script executable
chmod +x query.py

# Note: You may need to run the bash script with superuser permissions depending on your system's configuration
