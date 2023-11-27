#!/bin/bash

# update apt-get
apt-get update -y

# install python3 pip
apt-get install python3-pip -y

# install pymongo
pip3 install pymongo

# install mysql-connector-python
pip3 install mysql-connector-python
