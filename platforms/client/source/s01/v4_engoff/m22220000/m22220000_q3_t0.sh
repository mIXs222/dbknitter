#!/bin/bash

# Update package index
apt-get update

# Install python3 and pip3
apt-get install -y python3 python3-pip

# Install pymysql
pip3 install pymysql
