#!/bin/bash

# Update package list and install python3 and pip
sudo apt-get update
sudo apt-get install -y python3 python3-pip

# Install pymysql
python3 -m pip install pymysql
