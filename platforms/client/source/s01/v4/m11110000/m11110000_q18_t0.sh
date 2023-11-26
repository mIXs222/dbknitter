#!/bin/bash
# setup.sh

# Update the package list
sudo apt-get update

# Upgrade installed packages to latest versions
sudo apt-get upgrade

# Install Python3 and Pip if they are not installed
sudo apt-get install python3
sudo apt-get install python3-pip

# Install pymysql
pip3 install pymysql
