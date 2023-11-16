#!/bin/bash

# Install Python3 and pip if they are not installed
# This installation command works for Debian-based systems (e.g., Ubuntu)
sudo apt-get update
sudo apt-get install -y python3 python3-pip

# Install pymysql
pip3 install pymysql
