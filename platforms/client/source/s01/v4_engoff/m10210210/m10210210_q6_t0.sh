#!/bin/bash

# Update package list and install pip for Python 3
sudo apt-get update
sudo apt-get install -y python3-pip

# Install pymysql using pip
pip3 install pymysql
