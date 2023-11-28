#!/bin/bash

# Update the package lists
sudo apt-get update

# Install Python and Pip if they are not installed
sudo apt-get install -y python3 python3-pip

# Install python libraries
pip3 install pymysql pymongo pandas redis
