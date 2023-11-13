#!/bin/bash

# Update the system
apt update 

# Python and PIP (Python Installer)
apt install python3.8 python3-pip -y

# Python MySQL Library
pip3 install PyMySQL

# Python MongoDB Library
pip3 install pymongo

# Install pandas
pip3 install pandas

# Permissions for your Python Script
chmod +x your_python_script.py

# Run your Python Script
./your_python_script.py
