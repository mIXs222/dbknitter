#!/bin/bash

# Update package list and install Python and pip if they aren't already installed
sudo apt-get update
sudo apt-get install -y python3 python3-pip

# Install the required Python packages
pip3 install pandas
pip3 install direct_redis

