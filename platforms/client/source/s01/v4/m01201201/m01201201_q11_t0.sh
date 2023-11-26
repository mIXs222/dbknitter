#!/bin/bash

# Update package list
sudo apt-get update -y

# Install Python3, pip, and MySQL client if they are not installed
sudo apt-get install -y python3 python3-pip mysql-client

# Install pymongo and pymysql using pip
pip3 install pymongo pymysql
