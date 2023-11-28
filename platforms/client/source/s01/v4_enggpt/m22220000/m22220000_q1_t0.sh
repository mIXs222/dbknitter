#!/bin/bash
# This bash script is for installing all necessary dependencies for the Python code.

# Update the package list
sudo apt update

# Install Python and pip if they are not installed
which python || sudo apt install -y python
which pip || sudo apt install -y python3-pip

# Install PyMySQL
pip install pymysql
