#!/bin/bash

# Update package list and install pip and redis-server
sudo apt-get update
sudo apt-get install -y python3-pip redis-server

# Install MongoDB
wget -qO - https://www.mongodb.org/static/pgp/server-4.2.asc | sudo apt-key add -
echo "deb [ arch=amd64,arm64 ] https://repo.mongodb.org/apt/ubuntu bionic/mongodb-org/4.2 multiverse" | sudo tee /etc/apt/sources.list.d/mongodb-org-4.2.list
sudo apt-get update
sudo apt-get install -y mongodb-org

# Start MongoDB and Redis services
sudo systemctl start mongod
sudo systemctl enable mongod
sudo systemctl start redis-server
sudo systemctl enable redis-server

# Install necessary Python packages
pip3 install pymongo pandas direct_redis
