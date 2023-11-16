#!/bin/bash

# Updating the repo
sudo apt-get update

# Installing pip
sudo apt-get install python3-pip -y

# Installing python dependencies
pip3 install pandas
pip3 install pymysql
pip3 install pymongo
