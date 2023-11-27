#!/bin/bash

# Update the package index
sudo apt-get update

# Install python3 and python3-pip if they are not installed
sudo apt-get install -y python3 python3-pip

# Install PyMySQL using pip
pip3 install pymysql
