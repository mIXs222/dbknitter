#!/bin/bash

# Update package lists
sudo apt-get update

# Install Python3 and pip if not installed
sudo apt-get install -y python3 python3-pip
# Install pymysql library
pip3 install pymysql
