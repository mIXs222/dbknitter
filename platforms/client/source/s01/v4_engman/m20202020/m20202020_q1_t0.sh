#!/bin/bash

# Update package list and upgrade packages
sudo apt-get update -y
sudo apt-get upgrade -y

# Install pip if not installed
sudo apt-get install -y python3-pip

# Install pymysql
pip3 install pymysql
