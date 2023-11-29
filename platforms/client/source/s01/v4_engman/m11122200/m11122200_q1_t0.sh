#!/bin/bash
# Script to install all the dependencies needed to run the python code

# Update package list and Upgrade system
apt-get update -y && apt-get upgrade -y

# Install Python 3 and Pip
apt-get install python3 -y
apt-get install python3-pip -y

# Install pymysql using pip
pip3 install pymysql
