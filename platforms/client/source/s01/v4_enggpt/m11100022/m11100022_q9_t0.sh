#!/bin/bash

# Update repositories and install pip and Python development headers
sudo apt-get update
sudo apt-get install -y python3-pip python3-dev

# Install required libraries
pip3 install pymysql pymongo pandas "redis<4" direct_redis
