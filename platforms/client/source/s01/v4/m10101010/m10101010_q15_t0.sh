#!/bin/bash

# Update the package list
apt-get update

# Install Python 3 and pip if not already installed
apt-get install -y python3
apt-get install -y python3-pip

# Install PyMySQL
pip3 install pymysql
