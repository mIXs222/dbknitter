#!/bin/bash

# Update package list
apt update

# Install Python3 and Pip if they are not installed
apt install -y python3 python3-pip

# Install pymysql and pymongo using pip
pip3 install pymysql pymongo
