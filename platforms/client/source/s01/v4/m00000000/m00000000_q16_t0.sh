#!/bin/bash

# Update package lists
sudo apt-get update

# Install python3 and pip if they are not installed
sudo apt-get install -y python3 python3-pip

# Install PyMySQL
pip3 install pymysql
