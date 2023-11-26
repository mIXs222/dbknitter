#!/bin/bash

# Update the package lists
apt-get update

# Install Python3 and pip if they are not installed
apt-get install -y python3
apt-get install -y python3-pip

# Install the pymysql library
pip3 install pymysql
