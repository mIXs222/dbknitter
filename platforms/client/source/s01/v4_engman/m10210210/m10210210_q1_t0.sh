#!/bin/bash

# Update package index
apt-get update -y

# Install python3 and pip3 if not installed
apt-get install -y python3 python3-pip

# Install pymysql
pip3 install pymysql
