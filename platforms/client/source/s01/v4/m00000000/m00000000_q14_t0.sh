#!/bin/bash

# Update and install python3 and pip if not installed.
sudo apt-get update
sudo apt-get install -y python3
sudo apt-get install -y python3-pip

# Install pymysql Python library
pip3 install pymysql
