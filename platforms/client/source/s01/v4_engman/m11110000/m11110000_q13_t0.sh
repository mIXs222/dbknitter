#!/bin/bash

# Update package list
apt-get update

# Install Python and Pip if they are not installed
apt-get install -y python3
apt-get install -y python3-pip

# Install necessary Python packages
pip3 install pymysql
