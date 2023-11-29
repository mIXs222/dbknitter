#!/bin/bash

# Run this script to set up the Python environment and install dependencies.

# Update package list
apt-get update

# Install Python and pip if it's not already installed (you may skip if you have Python and pip)
apt-get install -y python3 python3-pip

# Install pymysql and pymongo using pip
pip3 install pymysql pymongo
