#!/bin/bash

# Update package list
apt-get update

# Upgrade packages
apt-get upgrade -y

# Install Python3 and pip if they are not installed
apt-get install python3-pip -y

# Install pymysql
pip3 install pymysql
