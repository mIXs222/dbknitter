#!/bin/bash

# Update package list
apt-get update

# Install Python and pip if they are not installed
apt-get install -y python3 python3-pip

# Install the pymysql library using pip
pip3 install pymysql
