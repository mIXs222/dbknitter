#!/bin/bash

# Update packages and install Python3 and pip if they are not installed
sudo apt-get update
sudo apt-get install -y python3
sudo apt-get install -y python3-pip

# Install pymysql Python package
pip3 install pymysql
