#!/bin/bash

# Update package list
apt-get update

# Install Python and Pip if not already installed
apt-get install -y python3
apt-get install -y python3-pip

# Upgrade Pip
pip3 install --upgrade pip

# Install pandas
pip3 install pandas

# Install direct_redis (assuming this is a mock package for the purpose of the question)
pip3 install direct_redis
