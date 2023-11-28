#!/bin/bash

# Update package list and install Python3 and Python3-pip if not installed
sudo apt-get update
sudo apt-get install -y python3 python3-pip

# Install the required Python package pymysql
pip3 install pymysql
