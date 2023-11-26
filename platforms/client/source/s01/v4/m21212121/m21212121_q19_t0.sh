#!/bin/bash

# Python environment setup
sudo apt-get update
sudo apt-get install -y python3 python3-pip
sudo apt-get install -y python3-pandas

# MongoDB driver
pip3 install pymongo
# Redis driver
pip3 install direct_redis
