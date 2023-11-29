#!/bin/bash

# Update and install system wide dependencies
apt-get update -y
apt-get install -y python3 python3-pip default-libmysqlclient-dev

# Install Python dependencies
pip3 install pymysql pandas redis direct_redis
