#!/bin/bash

# Update repositories
sudo apt-get update

# Install Python and pip if they're not already installed.
sudo apt-get install -y python3 python3-pip

# Install required Python libraries
pip3 install pymysql pymongo pandas
