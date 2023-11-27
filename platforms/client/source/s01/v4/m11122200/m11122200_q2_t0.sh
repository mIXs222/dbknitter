#!/bin/bash

# Update the package lists
sudo apt-get update

# Install pip if not already installed
sudo apt-get install python3-pip -y

# Install MongoDB driver
pip3 install pymongo

# Install Redis driver
pip3 install git+https://github.com/RedisJSON/direct_redis

# Install pandas
pip3 install pandas
