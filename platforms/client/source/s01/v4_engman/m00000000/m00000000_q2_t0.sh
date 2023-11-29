#!/bin/bash

# Update package list
sudo apt-get update

# Install Python3 and pip3 if not installed
sudo apt-get install -y python3 python3-pip

# Install pymysql library via pip
pip3 install pymysql
