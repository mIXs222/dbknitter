#!/bin/bash

# Update packages
apt-get update -y

# Install Python3 and pip3
apt-get install python3-pip -y

# Upgrade pip
pip3 install --upgrade pip

# Install required Python libraries
pip3 install pandas redis
