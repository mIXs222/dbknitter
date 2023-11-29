#!/bin/bash

# Update and Install pip and Python dev
apt-get update
apt-get install -y python3-pip python3-dev

# Install Python requirements
pip3 install pymongo
pip3 install direct_redis
pip3 install pandas
