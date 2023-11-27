#!/bin/bash

# Update package list
sudo apt-get update

# Install python3, pip and pymysql
sudo apt-get install -y python3 python3-pip
pip3 install pymysql
