#!/bin/bash

# Update the repositories and install Python3 and pip if not already installed
sudo apt-get update
sudo apt-get install -y python3 python3-pip

# Install pymysql using pip3
pip3 install pymysql
