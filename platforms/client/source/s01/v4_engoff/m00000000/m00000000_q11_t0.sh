#!/bin/bash

# Update package list
sudo apt-get update

# Install pip for Python 3
sudo apt-get install -y python3-pip

# Install pymysql via pip
pip3 install pymysql
