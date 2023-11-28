#!/bin/bash

# Bash script to install all dependencies to run the Python code

# Update package list
apt-get update

# Install pip if not installed
apt-get install -y python3-pip

# Install required Python packages
pip3 install pymongo
pip3 install direct-redis
pip3 install pandas
