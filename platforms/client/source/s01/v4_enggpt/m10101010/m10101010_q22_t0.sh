#!/bin/bash

# Update package list
sudo apt-get update

# Install Python 3 and pip if not installed
sudo apt-get install -y python3 python3-pip

# Install pymysql, pymongo, pandas
pip3 install pymysql pymongo pandas
