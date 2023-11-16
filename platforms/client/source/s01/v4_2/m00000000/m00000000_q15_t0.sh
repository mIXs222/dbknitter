#!/bin/bash

# update packages list and upgrade all packages
sudo apt update
sudo apt upgrade -y

# install python3 and pip
sudo apt install python3 -y
sudo apt install python3-pip -y

# install pymysql
pip3 install pymysql

# install pandas
pip3 install pandas

# install sqlalchemy
pip3 install sqlalchemy
