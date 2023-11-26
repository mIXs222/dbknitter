#!/bin/bash

# Update package list and install Python3 and pip if they are not installed
sudo apt update
sudo apt install python3 python3-pip -y

# Install pymysql Python package
pip3 install pymysql
