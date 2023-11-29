#!/bin/bash

# Update package lists
sudo apt-get update

# Install pip if it's not already installed
sudo apt-get install -y python3-pip

# Install the pymongo package
pip3 install pymongo

# Install the pymysql package
pip3 install pymysql

# Install the csv package (usually included in Python's standard libraries)
# No action needed, included for completeness
