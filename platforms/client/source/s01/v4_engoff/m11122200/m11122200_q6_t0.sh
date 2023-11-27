#!/bin/bash

# Update package list
sudo apt-get update

# Install Python if not already installed (assuming you are using a Debian-based system)
sudo apt-get install -y python3

# Install pip if not already installed
sudo apt-get install -y python3-pip

# Install pymysql using pip
pip3 install pymysql
