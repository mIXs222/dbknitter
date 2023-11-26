#!/bin/bash

# Update package list and upgrade existing packages
sudo apt-get update
sudo apt-get -y upgrade

# Install Python 3 and pip if they are not installed
sudo apt-get install -y python3 python3-pip

# Install pymysql library using pip
pip3 install pymysql
