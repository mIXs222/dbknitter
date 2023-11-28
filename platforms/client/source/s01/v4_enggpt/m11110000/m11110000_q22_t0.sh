#!/bin/bash

# Update package list and install python3 and pip
sudo apt-get update
sudo apt-get install -y python3
sudo apt-get install -y python3-pip

# Install pymysql for Python3
pip3 install pymysql
