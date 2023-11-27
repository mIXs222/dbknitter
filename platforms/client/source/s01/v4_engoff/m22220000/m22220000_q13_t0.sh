#!/bin/bash

# Updating and Installing Python3 and PIP if not already installed
sudo apt-get update
sudo apt-get install -y python3
sudo apt-get install -y python3-pip

# Upgrade PIP
pip3 install --upgrade pip

# Install PyMySQL using pip
pip3 install pymysql
