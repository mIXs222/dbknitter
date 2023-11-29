#!/bin/bash

# Update repositories and install python3 and pip
sudo apt-get update
sudo apt-get install -y python3
sudo apt-get install -y python3-pip

# Install pymysql
pip3 install pymysql
