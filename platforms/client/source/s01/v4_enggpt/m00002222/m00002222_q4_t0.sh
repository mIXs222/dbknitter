#!/bin/bash

# Update package list and upgrade packages
sudo apt-get update && sudo apt-get upgrade -y

# Install Python 3 and pip if not already installed
sudo apt-get install -y python3 python3-pip

# Install the required Python modules
pip3 install pandas
pip3 install direct_redis
