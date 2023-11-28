#!/bin/bash

# Install python, pip, redis, mysql and mongodb
sudo apt-get update
sudo apt-get install -y python3 python3-pip

# Upgrade pip
pip3 install --upgrade pip

# Install required Python libraries
pip3 install pymysql pymongo pandas sqlalchemy direct_redis
