#!/bin/bash

# Install MySQL client
apt-get update && apt-get install -y default-mysql-client

# Install Python environment and dependencies
apt-get install -y python3 python3-pip
pip3 install pandas pymysql pymongo direct_redis
