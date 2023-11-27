#!/bin/bash

# Update package lists
sudo apt-get update

# Upgrade packages
sudo apt-get upgrade

# Install Python3 and pip if they are not installed
sudo apt-get install -y python3 python3-pip

# Install pymysql using pip
pip3 install pymysql
