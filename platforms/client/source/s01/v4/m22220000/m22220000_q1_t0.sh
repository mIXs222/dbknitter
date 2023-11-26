#!/bin/bash

# Update package lists
apt-get update

# Install python3, pip and pymysql
apt-get install -y python3 python3-pip
pip3 install pymysql
