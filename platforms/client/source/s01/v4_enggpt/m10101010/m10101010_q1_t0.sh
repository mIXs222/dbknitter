#!/bin/bash

# Update the system
sudo apt-get update

# Install Python3 and pip if not present
sudo apt-get install python3 -y
sudo apt-get install python3-pip -y

# Install pymysql package
pip3 install pymysql
