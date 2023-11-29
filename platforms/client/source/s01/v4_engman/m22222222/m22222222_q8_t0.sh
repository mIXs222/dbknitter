#!/bin/bash

# Update the system's package index
sudo apt-get update

# Install pip for python3
sudo apt-get install -y python3-pip

# Install Pandas and its dependencies
pip3 install pandas

# Install direct_redis package
pip3 install git+https://github.com/bmuller/direct_redis.git
