#!/bin/bash

# Update the package lists for upgrades and new package installations
sudo apt-get update

# Firstly, update Python to the latest version
sudo apt-get upgrade python3

# Install pip, the Python package installer
sudo apt-get install python3-pip

# Now use pip to install the necessary Python packages.
# mysql-connector-python is used to connect our Python script to the MySQL database,
# while pandas and sqlalchemy are used to manipulate the data and write to CSV.
pip install mysql-connector-python pandas sqlalchemy
