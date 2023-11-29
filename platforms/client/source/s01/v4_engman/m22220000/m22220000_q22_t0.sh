#!/bin/bash

# Update package lists
sudo apt update

# Install pip for Python3
sudo apt install -y python3-pip

# Install pymysql using pip
pip3 install pymysql
