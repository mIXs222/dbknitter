#!/bin/bash

# Update package list and upgrade packages
apt-get update
apt-get -y upgrade

# Install pip and Python development files
apt-get install -y python3-pip python3-dev

# Install the necessary Python libraries
pip3 install pymongo pandas direct_redis
