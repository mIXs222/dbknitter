#!/bin/bash

# Update the package list
sudo apt-get update

# Install Python and pip
sudo apt-get install -y python3 python3-pip

# Install pymysql
pip3 install pymysql
