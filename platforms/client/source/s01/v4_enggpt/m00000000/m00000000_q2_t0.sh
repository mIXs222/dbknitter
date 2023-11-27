#!/bin/bash

# Update package list
sudo apt-get update

# Upgrade existing packages
sudo apt-get upgrade -y

# Install Python3 and pip if not already installed
sudo apt-get install -y python3 python3-pip

# Install pymysql
pip3 install pymysql
