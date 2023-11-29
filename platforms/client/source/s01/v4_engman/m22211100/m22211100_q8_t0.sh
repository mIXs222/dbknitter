#!/bin/bash

# Update and install pip and Python development headers
sudo apt-get update
sudo apt-get install -y python3-pip python3-dev

# Install the necessary Python libraries
pip3 install pymysql pymongo pandas direct_redis
