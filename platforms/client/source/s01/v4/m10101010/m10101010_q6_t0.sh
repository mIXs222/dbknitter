#!/bin/bash

# Update the package list
apt-get update -y

# Install Python and pip if not already installed
apt-get install python3 python3-pip -y

# Install the pymysql library
pip3 install pymysql
