#!/bin/bash

# Update the package list
apt-get update

# Install Python 3 and pip if they aren't already installed
apt-get install python3 python3-pip -y

# Install the pymongo package
pip3 install pymongo

# Install pandas
pip3 install pandas

# Install the direct_redis package
pip3 install git+https://github.com/patrikoss/direct_redis.git
