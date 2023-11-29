#!/bin/bash

# install_dependencies.sh

# Update repositories and make sure Python3 is installed
sudo apt update
sudo apt install python3 -y

# Ensure pip is installed
sudo apt install python3-pip -y

# Install PyMySQL library
pip3 install pymysql
