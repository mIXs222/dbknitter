#!/bin/bash

# Update package list
sudo apt-get update

# Install Python3 and Pip if not present
sudo apt-get install -y python3 python3-pip

# Install MongoDB driver (pymongo)
pip3 install pymongo

# Install pandas
pip3 install pandas

# Install redis (and direct_redis that was specifically requested)
pip3 install redis direct_redis
