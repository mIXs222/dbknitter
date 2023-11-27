#!/bin/bash

# update packages
apt-get update -y

# install pip
apt-get install python3-pip -y

# install MySQL client
apt-get install mysql-client -y

# install required python packages
pip3 install pymysql pandas
