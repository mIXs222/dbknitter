#!/bin/bash

# Update the package list
apt-get update

# Install pip and Python development packages (if not already installed)
apt-get install -y python3-pip python3-dev

# Upgrade pip
pip3 install --upgrade pip

# Install pymongo
pip3 install pymongo
