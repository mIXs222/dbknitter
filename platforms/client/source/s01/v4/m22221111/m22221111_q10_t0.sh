#!/bin/bash

# Update package list and upgrade packages
sudo apt-get update
sudo apt-get -y upgrade

# Install Python3 pip
sudo apt-get install -y python3-pip

# Install MongoDB
wget -qO - https://www.mongodb.org/static/pgp/server-4.4.asc | sudo apt-key add -
echo "deb http://repo.mongodb.org/apt/ubuntu bionic/mongodb-org/4.4 multiverse" | sudo tee /etc/apt/sources.list.d/mongodb-org-4.4.list
sudo apt-get update
sudo apt-get install -y mongodb-org

# Install Redis
sudo apt-get install -y redis-server

# Install pymongo
pip3 install pymongo

# Install pandas
pip3 install pandas

# Install direct_redis
pip3 install git+https://github.com/packagename/direct_redis.git

# Ensure Redis and MongoDB are started
sudo systemctl start mongod
sudo systemctl enable mongod
sudo systemctl start redis-server
sudo systemctl enable redis-server
