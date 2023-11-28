#!/bin/bash
# setup.sh

# update package lists
sudo apt-get update

# install python3 if not installed
sudo apt-get install -y python3

# install pip3
sudo apt-get install -y python3-pip

# use pip3 to install pymysql
pip3 install pymysql
