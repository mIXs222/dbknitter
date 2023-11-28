#!/bin/bash

# Install Python and pip if not already installed

# Update package list and install pip
sudo apt-get update
sudo apt-get -y install python3-pip

# Install Python packages
pip3 install pymysql pymongo pandas direct-redis
