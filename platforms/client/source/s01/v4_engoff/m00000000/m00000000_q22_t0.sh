#!/bin/bash

# Update the package index
sudo apt-get update

# Install pip for Python 3
sudo apt-get install -y python3-pip

# Install pymysql using pip
pip3 install pymysql
