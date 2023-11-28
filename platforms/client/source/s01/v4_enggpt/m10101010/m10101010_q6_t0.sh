#!/bin/bash

# install_dependencies.sh

# Update package list and install Python and pip if not already installed
sudo apt-get update
sudo apt-get install -y python3
sudo apt-get install -y python3-pip

# Install pymysql Python library
pip3 install pymysql
