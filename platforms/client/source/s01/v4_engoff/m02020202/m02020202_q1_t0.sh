#!/bin/bash

# Update package list and upgrade packages
sudo apt-get update
sudo apt-get -y upgrade

# Install pip for Python 3
sudo apt-get install -y python3-pip

# Ensure pip, setuptools, and wheel are up to date
pip3 install --upgrade pip setuptools wheel

# Install Python packages required for the script
pip3 install pandas redis_direct
