#!/bin/bash

# Bash script to install all dependencies

# Install MySQL client (for MySQL DB)
sudo apt-get update
sudo apt-get install -y mysql-client

# Install the necessary python libraries
pip install pandas pymysql pymongo redis sqlalchemy direct_redis
