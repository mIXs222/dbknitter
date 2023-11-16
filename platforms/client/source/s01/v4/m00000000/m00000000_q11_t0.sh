#!/bin/bash

# Update package list and upgrade packages
sudo apt-get update
sudo apt-get -y upgrade

# Install Python3 and pip if they are not installed
sudo apt-get install -y python3
sudo apt-get install -y python3-pip

# Install pymysql package
pip3 install pymysql
