#!/bin/bash

# Update package lists
sudo apt-get update

# Install pip if it is not installed
which pip || sudo apt install python3-pip -y

# Install the pymysql library using pip
pip install pymysql
