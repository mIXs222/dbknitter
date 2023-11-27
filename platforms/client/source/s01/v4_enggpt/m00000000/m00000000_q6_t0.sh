#!/bin/bash
# install_dependencies.sh

# Update the package list
sudo apt-get update

# Install Python3 and Python3-pip if they are not installed
sudo apt-get install -y python3 python3-pip

# Install pymysql
pip3 install pymysql
