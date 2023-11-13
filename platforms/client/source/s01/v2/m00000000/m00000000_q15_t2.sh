#!/bin/bash

# Update the package list
sudo apt update

# Install python3 and pip
sudo apt install python3 -y
sudo apt install python3-pip -y

# Install mysql.connector and pandas
pip3 install mysql-connector-python pandas
