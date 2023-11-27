#!/bin/bash

# Update package list
apt-get update

# Upgrade packages
apt-get upgrade -y

# Install Python and pip
apt-get install -y python3 python3-pip

# Install pymongo
pip3 install pymongo
