#!/bin/bash 

# Update the package lists for upgrades and new package installations
sudo apt-get update 

# Install Python3 and Pip3
sudo apt-get install python3.8 python3-pip --yes 

# Install the required Python packages
pip3 install pandas
pip3 install pymysql
