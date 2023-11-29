#!/bin/bash

# Update package list
sudo apt-get update

# Install Python3 if not already installed
sudo apt-get install -y python3

# Install pip for Python3 if not already installed
sudo apt-get install -y python3-pip

# Install pymysql library
pip3 install pymysql
