#!/bin/bash

# Update package list
sudo apt-get update

# Install Python 3 and Pip
sudo apt-get install -y python3
sudo apt-get install -y python3-pip

# Install PyMySQL
pip3 install pymysql
