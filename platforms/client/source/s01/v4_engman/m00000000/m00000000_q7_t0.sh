#!/bin/bash

# Update package list
sudo apt-get update

# Install Python 3 and pip if they are not installed
sudo apt-get install -y python3 python3-pip

# Install pymysql (MySQL database connector for Python)
pip3 install pymysql
