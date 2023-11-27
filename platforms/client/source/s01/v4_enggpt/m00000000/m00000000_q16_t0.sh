#!/bin/bash

# Update package list and upgrade existing packages
sudo apt-get update -y
sudo apt-get upgrade -y

# Install Python3 and pip if they are not installed
sudo apt-get install python3 -y
sudo apt-get install python3-pip -y

# Install pymysql using pip
pip3 install pymysql
