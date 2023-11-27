#!/bin/bash

# Updating package list
sudo apt-get update

# Install Python3 pip
sudo apt-get install -y python3-pip

# Install pymysql using pip
pip3 install pymysql
