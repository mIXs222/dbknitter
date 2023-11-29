#!/bin/bash

# Update the package list
apt-get update

# Install Python3 and pip3 if not installed
apt-get install -y python3 python3-pip

# Install pymysql
pip3 install pymysql
