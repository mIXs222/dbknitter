#!/bin/bash

# Update package list
sudo apt-get update

# Upgrade packages
sudo apt-get upgrade

# Install Python and pip
sudo apt-get install python3 python3-pip

# Install pymysql
pip3 install pymysql
