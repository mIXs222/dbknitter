#!/bin/bash
# Bash script to install dependencies for Python code execution

# Update repositories and upgrade packages
sudo apt-get update
sudo apt-get upgrade -y

# Install pip for Python 3
sudo apt-get install python3-pip -y

# Install pymongo & redis for Python
pip3 install pymongo redis pandas
