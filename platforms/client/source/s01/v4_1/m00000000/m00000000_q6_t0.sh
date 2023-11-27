#!/bin/bash

# install python3
sudo apt-get update
sudo apt-get install -y python3.8

# install pip3
sudo apt-get install -y python3-pip

# install pymysql, pandas and csv packages
pip3 install pymysql pandas
