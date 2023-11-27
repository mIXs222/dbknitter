#!/bin/bash
# dependencies.sh

# Update package list
sudo apt-get update

# Install MySQL client, Python, pip and necessary C headers
sudo apt-get install -y mysql-client python3 python3-pip python3-dev libssl-dev

# Install MongoDB
wget -qO - https://www.mongodb.org/static/pgp/server-4.4.asc | sudo apt-key add -
echo "deb [ arch=amd64,arm64 ] http://repo.mongodb.org/apt/ubuntu bionic/mongodb-org/4.4 multiverse" | sudo tee /etc/apt/sources.list.d/mongodb-org-4.4.list
sudo apt-get update
sudo apt-get install -y mongodb-org

# Install Redis
sudo apt-get install -y redis-server

# Install Python libraries
pip3 install pymysql pymongo pandas direct_redis

# Set up Redis for Python
git clone https://github.com/internetimagery/direct-redis.git
cd direct-redis
python3 setup.py install
