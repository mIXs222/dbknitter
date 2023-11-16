#!/bin/bash

# update and upgrade the system
sudo apt-get update -y
sudo apt-get upgrade -y

# install python3 & pip3
sudo apt-get install python3.8 -y
sudo apt-get install python3-pip -y

# install python libraries
pip3 install pymysql pandas
