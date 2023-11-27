#!/bin/bash

# update package lists
sudo apt-get update -y

# Install pip for Python 3
sudo apt-get install python3-pip -y

# Install pymongo
pip3 install pymongo

# Install mysql-connector
pip3 install mysql-connector-python
