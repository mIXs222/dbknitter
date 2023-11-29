#!/bin/bash

# Update package lists
sudo apt-get update

# Make sure python3 and pip are installed
sudo apt-get install -y python3 python3-pip

# Install Pandas
pip3 install pandas

# Install Direct Redis
pip3 install direct-redis
