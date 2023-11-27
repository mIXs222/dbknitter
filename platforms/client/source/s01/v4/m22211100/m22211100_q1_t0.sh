#!/bin/bash
# install_dependencies.sh

# Ensure pip is installed
sudo apt update
sudo apt install -y python3-pip

# Install pymysql
pip3 install pymysql
