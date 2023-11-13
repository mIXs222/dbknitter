#!/bin/bash
# This script will install the necessary dependencies

echo "Updating packages"
sudo apt-get update -y  

echo "Installing Python 3 pip"
sudo apt-get install python3-pip -y  

echo "Installing necessary Python libraries"
pip3 install sqlalchemy pandas pymysql 

echo "All necessary software has been installed"
