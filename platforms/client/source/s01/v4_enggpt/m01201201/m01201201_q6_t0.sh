#!/bin/bash

# Update the package list
apt-get update

# Install pip if not already installed
apt-get install -y python3-pip

# Upgrade pip to the latest version
pip3 install --upgrade pip

# Install pymongo
pip3 install pymongo
