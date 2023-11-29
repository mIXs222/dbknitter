#!/bin/bash
# Bash script to install dependencies for the Python script

sudo apt update
sudo apt install -y python3 python3-pip
pip3 install pymysql pymongo
