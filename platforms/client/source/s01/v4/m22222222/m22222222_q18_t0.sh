#!/bin/bash

# Update package list and install pip if it's not already installed
sudo apt-get update
sudo apt-get install -y python3-pip

# Install pandas package using pip
pip3 install pandas

# Install direct_redis from git
pip3 install git+https://github.com/Joshuaalbert/direct_redis.git
