#!/bin/bash

# Update package list
apt-get update

# Install Python pip if not installed
apt-get install -y python3-pip

# Install PyMySQL
pip3 install pymysql

# Install pymongo
pip3 install pymongo

# Make sure pip is up-to-date
pip3 install --upgrade pip
