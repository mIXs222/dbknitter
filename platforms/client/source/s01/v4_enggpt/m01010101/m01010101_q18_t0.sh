#!/bin/bash

# update the package repository
sudo apt-get update

# install Python3 and PIP
sudo apt-get install python3 python3-pip -y

# install pymysql and pymongo using PIP
pip3 install pymysql pymongo
