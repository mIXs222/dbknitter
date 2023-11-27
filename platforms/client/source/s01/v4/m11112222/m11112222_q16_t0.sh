#!/bin/bash

# Update system
sudo apt-get update

# Install Python3 and Pip if they are not installed
sudo apt-get install -y python3 python3-pip

# Install pymongo, redis, pandas using pip
pip3 install pymongo redis pandas direct_redis
