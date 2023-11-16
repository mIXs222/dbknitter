#!/bin/bash
# File: install_dependencies.sh

apt-get update
apt-get install -y python3
apt-get install -y python3-pip
pip3 install pymysql
