#!/bin/bash
# Update package list and upgrade all packages
sudo apt-get update
sudo apt-get upgrade

# Ensure python3 and pip is installed
sudo apt-get install python3.8
sudo apt-get install python3-pip

# pymongo package installation
pip install pymongo

# pandas package installation
pip install pandas
