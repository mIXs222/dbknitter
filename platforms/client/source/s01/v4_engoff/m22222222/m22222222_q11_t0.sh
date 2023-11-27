#!/bin/bash

# Update the package list
sudo apt-get update

# Install pip for Python3 if it's not already installed
sudo apt-get install -y python3-pip

# Install pandas
pip3 install pandas

# Install direct_redis (assuming direct_redis can be installed via pip)
pip3 install direct_redis

# Verify if csv module needs installation. It's part of Python's standard library from version 2.3
