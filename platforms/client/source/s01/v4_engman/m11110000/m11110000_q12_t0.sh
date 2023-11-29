#!/bin/bash

# Update package lists
sudo apt update

# Install Python 3 and pip if not already installed
sudo apt install python3 python3-pip -y

# Install PyMySQL
pip3 install pymysql
