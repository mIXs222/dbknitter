#!/bin/bash

# Update package list and install pip if not already installed
sudo apt-get update
sudo apt-get install -y python3-pip

# Install pandas
pip3 install pandas

# Install direct_redis (assuming it's available in pip repository)
pip3 install direct_redis
