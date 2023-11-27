#!/bin/bash

# Update the package list
sudo apt-get update

# Install Python and pip
sudo apt-get install -y python3 python3-pip

# Install the required Python packages
pip3 install pymysql direct_redis pandas
