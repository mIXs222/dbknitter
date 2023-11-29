#!/bin/bash

# Install Python 3 and pip if they are not installed
sudo apt update
sudo apt install -y python3 python3-pip

# Install pymysql using pip
pip3 install pymysql
