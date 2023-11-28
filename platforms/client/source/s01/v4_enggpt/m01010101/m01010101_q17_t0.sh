#!/bin/bash

# Update the package list
sudo apt-get update

# Upgrade packages
sudo apt-get upgrade -y

# Install Python 3 and pip if they are not installed
sudo apt-get install -y python3 python3-pip

# Install Python libraries
pip3 install pymysql pymongo pandas
