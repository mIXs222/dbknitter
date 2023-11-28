#!/bin/bash

# Update package list and upgrade packages
apt-get update
apt-get -y upgrade

# Install Python 3 and pip if they are not installed
apt-get install -y python3 python3-pip

# Install PyMySQL
pip3 install pymysql
