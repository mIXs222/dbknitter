#!/bin/bash

# update package lists
sudo apt update

# install mysql connector for python
sudo apt install python3-pip
pip3 install mysql-connector-python-rf

# install csv module for python
pip3 install python-csv
