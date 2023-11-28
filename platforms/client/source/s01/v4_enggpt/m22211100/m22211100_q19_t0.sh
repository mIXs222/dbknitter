#!/bin/bash
# Install dependencies
sudo apt-get update
sudo apt-get install -y python3 python3-pip python3-dev default-libmysqlclient-dev build-essential

# Install python libraries
pip3 install pymysql pandas direct_redis
