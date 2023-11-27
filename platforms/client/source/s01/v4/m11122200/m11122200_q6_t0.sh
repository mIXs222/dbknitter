#!/bin/bash
# A script to install the PyMySQL dependency

# Update package list
sudo apt-get update

# Upgrade packages
sudo apt-get upgrade

# Install pip for Python3
sudo apt-get install python3-pip -y

# Install the PyMySQL library
pip3 install pymysql
