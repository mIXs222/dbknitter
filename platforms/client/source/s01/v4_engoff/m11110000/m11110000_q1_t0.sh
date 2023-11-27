#!/bin/bash

# Update package lists
sudo apt-get update

# Install python3
sudo apt-get install -y python3

# Install pip for Python 3
sudo apt-get install -y python3-pip

# Install pymysql
pip3 install pymysql
