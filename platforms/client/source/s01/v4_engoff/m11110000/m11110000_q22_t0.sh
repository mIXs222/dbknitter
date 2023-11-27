#!/bin/bash

# Updating package list
apt-get update

# Installing Python and pip if they are not already installed
apt-get install -y python3 python3-pip

# Upgrade pip to the latest version
pip3 install --upgrade pip

# Installing pymysql library using pip
pip3 install pymysql
