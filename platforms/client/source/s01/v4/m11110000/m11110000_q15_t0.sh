#!/bin/bash

# Update the package list
sudo apt-get update

# Upgrade packages
sudo apt-get upgrade

# Install pip for Python3
sudo apt-get install python3-pip

# Install pymysql
pip3 install pymysql

# Install pymongo
pip3 install pymongo
