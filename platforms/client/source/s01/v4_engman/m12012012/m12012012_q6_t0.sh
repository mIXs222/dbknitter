#!/bin/bash

# Update package list
apt-get update

# Install pip if not already installed
apt-get install -y python3-pip

# Install Redis dependencies
apt-get install -y redis-server

# Install Python packages
pip3 install pandas
pip3 install direct_redis
