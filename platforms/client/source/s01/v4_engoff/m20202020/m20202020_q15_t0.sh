#!/bin/bash

# Update package list and upgrade all packages
sudo apt-get update
sudo apt-get -y upgrade

# Install Python3 and pip if not present
sudo apt-get install -y python3
sudo apt-get install -y python3-pip

# Install pymysql Python package
pip3 install pymysql
