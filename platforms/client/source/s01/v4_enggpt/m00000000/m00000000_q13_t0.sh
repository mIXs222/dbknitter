#!/bin/bash

# Updating package index
sudo apt-get update -y

# Installing Python3 and pip3 if they aren't installed
sudo apt-get install -y python3 python3-pip

# Install MySQL client (just in case we need to connect to MySQL DB from terminal)
sudo apt-get install -y mysql-client

# Installing the necessary Python libraries
pip3 install pymysql
