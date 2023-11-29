#!/bin/bash
# install_dependencies.sh

# Update package list
sudo apt-get update

# Install Python3 and pip if not installed
sudo apt-get install python3 python3-pip -y

# Install pymysql
pip3 install pymysql
