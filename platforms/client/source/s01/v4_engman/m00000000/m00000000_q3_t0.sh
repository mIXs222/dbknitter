#!/bin/bash
# install.sh

# Update repositories and install Python3 and pip
sudo apt-get update -y
sudo apt-get install python3 -y
sudo apt-get install python3-pip -y

# Install pymysql library
pip3 install pymysql
