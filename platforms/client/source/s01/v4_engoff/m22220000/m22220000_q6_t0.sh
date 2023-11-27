#!/bin/bash

# Update the package list
sudo apt-get update

# Install Python3 if not already installed
sudo apt-get install -y python3

# Install pip for Python3 if not already installed
sudo apt-get install -y python3-pip

# Install PyMySQL package using pip
pip3 install pymysql
