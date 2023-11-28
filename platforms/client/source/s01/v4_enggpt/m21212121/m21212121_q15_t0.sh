#!/bin/bash

# Update the package list
apt-get update

# Upgrade packages
apt-get upgrade

# Install Python3 and pip
apt-get install -y python3 python3-pip

# Install pymongo
pip3 install pymongo
