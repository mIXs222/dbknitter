#!/bin/bash
# Installing Python and pip if not already installed
sudo apt update
sudo apt install python3 python3-pip -y

# Installing necessary Python libraries
pip3 install pymysql pandas redis
