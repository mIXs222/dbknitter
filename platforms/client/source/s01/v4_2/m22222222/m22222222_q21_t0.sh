#!/bin/bash

# Update the package lists
sudo apt-get update

# Install Python and pip
sudo apt-get install -y python3
sudo apt-get install -y python3-pip

# Install Pandas
pip3 install pandas

# Install Redis
pip3 install redis
