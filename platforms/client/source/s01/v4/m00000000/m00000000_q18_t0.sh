#!/bin/bash

# Update package list
sudo apt-get update

# Install Python3 and Pip3 if not already installed
sudo apt-get install -y python3 python3-pip

# Install Python MySQL connector PyMySQL
pip3 install pymysql
