#!/bin/bash

# Bash script to install all required dependencies

# Update the package list
apt-get update

# Install pip for Python package management
apt-get install -y python3-pip

# Install MongoDB driver
pip3 install pymongo

# Install pandas
pip3 install pandas

# Install numpy
pip3 install numpy

# Install Redis client packages
pip3 install redis direct-redis
