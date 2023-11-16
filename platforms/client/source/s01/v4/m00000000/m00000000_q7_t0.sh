#!/bin/bash

# Update package list
apt-get update

# Install Python3 and Pip
apt-get install -y python3 python3-pip

# Install pymysql
pip3 install pymysql
