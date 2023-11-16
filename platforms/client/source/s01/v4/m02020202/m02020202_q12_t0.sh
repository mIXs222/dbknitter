#!/bin/bash

# This script will install the dependencies required for the Python script.

# Update repositories
sudo apt-get update

# Install Python and pip if they are not already installed
sudo apt-get install -y python3 python3-pip

# Install the necessary Python packages
pip3 install pandas pymysql direct-redis
