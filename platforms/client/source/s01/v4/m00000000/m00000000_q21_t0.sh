#!/bin/bash

# Update package lists
sudo apt-get update

# Upgrade existing packages
sudo apt-get upgrade -y

# Install Python and Pip if not already installed
sudo apt-get install python3 -y
sudo apt-get install python3-pip -y

# Install the required Python package using pip
pip3 install pymysql
