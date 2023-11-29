#!/bin/bash

# Install Python, pip, and required packages
apt-get update
apt-get install -y python3 python3-pip
pip3 install pymysql pymongo pandas direct_redis
