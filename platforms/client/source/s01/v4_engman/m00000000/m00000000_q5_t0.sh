#!/bin/bash

# Update package list and install python3, python3-pip
sudo apt-get update
sudo apt-get install -y python3 python3-pip

# Install pymysql via pip
pip3 install pymysql
