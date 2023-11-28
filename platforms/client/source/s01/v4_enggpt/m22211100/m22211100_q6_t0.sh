#!/bin/bash

# Update package lists
sudo apt-get update

# Install Python and pip if not already installed
sudo apt-get install -y python3 python3-pip

# Install pymysql
pip3 install pymysql
