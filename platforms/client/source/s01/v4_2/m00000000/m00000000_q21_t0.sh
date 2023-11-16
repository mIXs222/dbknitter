#!/bin/bash

# Update the package lists
sudo apt-get update

# Install python
sudo apt-get install python3.8 -y

# Install pip
sudo apt-get install python3-pip -y

# Install MySQL server
sudo apt-get install mysql-server -y

# Install Python's pymysql library
pip3 install pymysql

# Install pandas
pip3 install pandas

# Install sqlalchemy
pip3 install sqlalchemy 

# Give permissions to execute the python script
chmod +x query_exec.py

# Execute the python script
python3 query_exec.py
