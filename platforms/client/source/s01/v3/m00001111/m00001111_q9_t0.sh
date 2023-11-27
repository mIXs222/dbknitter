#!/bin/bash

# Updating packages and installing python3
sudo apt update -y && sudo apt upgrade -y
sudo apt install python3 python3-pip -y

# Install python libraries
pip3 install mysql-connector-python
pip3 install pymongo
pip3 install pandas
