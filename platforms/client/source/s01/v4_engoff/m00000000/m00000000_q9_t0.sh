#!/bin/bash
# Bash script to install python3 and pymysql

# Update the package list
sudo apt-get update

# Install python3, pip and MySQL development headers needed for MySQL client library
sudo apt-get install -y python3 python3-pip default-libmysqlclient-dev

# Install pymysql using pip
pip3 install pymysql
