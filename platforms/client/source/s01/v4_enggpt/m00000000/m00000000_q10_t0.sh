#!/bin/bash

# Update package lists
sudo apt-get update

# Install Python and pip
sudo apt-get install -y python3 python3-pip

# Install PyMySQL (add any other dependencies here if needed)
pip3 install pymysql
