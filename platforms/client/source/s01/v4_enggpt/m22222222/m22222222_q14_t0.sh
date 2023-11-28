#!/bin/bash

# Update the package list
apt-get update

# Install Python pip if not already installed
apt-get install -y python3-pip

# Install Python libraries required for the script
pip3 install pandas numpy direct_redis
