#!/bin/bash

# Update package list
apt-get update -y

# Install Python 3 and pip if not already installed
apt-get install python3 -y
apt-get install python3-pip -y

# Install pymysql package
pip3 install pymysql
