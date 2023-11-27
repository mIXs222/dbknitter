#!/bin/bash

# Update package list
sudo apt-get update

# Install Python and pip
sudo apt-get install -y python3 python3-pip

# Install MongoDB
sudo apt-get install -y mongodb

# Install Redis
sudo apt-get install -y redis-server

# Install Python packages for MongoDB and Redis
pip3 install pymongo
pip3 install pandas
pip3 install direct_redis

# The script assumes that the MongoDB and Redis servers are accessible and that no additional setup is necessary.
# If you are running this script in a new environment where MongoDB and Redis servers are not setup,
# additional steps might be needed to install and configure these services.
