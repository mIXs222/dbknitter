#!/bin/bash

# Update package list
sudo apt-get update

# Install Python if it's not already installed
sudo apt-get install -y python3

# Install pip if it's not already installed
sudo apt-get install -y python3-pip

# Install pandas
pip3 install pandas

# Install the direct_redis package (assuming this is a custom package that exists)
pip3 install direct_redis
