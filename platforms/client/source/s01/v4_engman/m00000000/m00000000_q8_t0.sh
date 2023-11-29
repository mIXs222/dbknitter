#!/bin/bash

# Update package lists
apt-get update

# Upgrade packages
apt-get upgrade -y

# Install Python and pip
apt-get install python3 python3-pip -y

# Install pymysql library using pip
pip3 install pymysql
