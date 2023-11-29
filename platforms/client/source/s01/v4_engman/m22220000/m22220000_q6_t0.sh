#!/bin/bash
# File: install_dependencies.sh

# Update and install pip if it's not already installed
sudo apt-get update
sudo apt-get install -y python3-pip

# Install pymysql
pip3 install pymysql
