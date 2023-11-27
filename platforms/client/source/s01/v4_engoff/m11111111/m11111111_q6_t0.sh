#!/bin/bash

# Update package manager
apt-get update

# Upgrade existing packages
apt-get upgrade -y

# Install Python 3 and pip
apt-get install python3 -y
apt-get install python3-pip -y

# Install pymongo
pip3 install pymongo
